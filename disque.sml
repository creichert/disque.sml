
(*

Standard ML Disque Client

NOTES:

- TODO more robust protocol parsing
- TODO auth
- TODO Pipelining

*)

(* stdin/stdout logger *)
structure log : sig
    type t
    val debug : t -> unit
    val error : t -> unit
end = struct

type t = string
val debug = fn x => TextIO.output (TextIO.stdOut, x)
val error = fn x => TextIO.output (TextIO.stdErr, x)

end

(* Disque/Redis Parsing *)
structure DisqueParser :
  sig
  datatype reply = SingleLine of string                 (* '+' *)
		 | Error      of string                 (* '-' *)
		 | Integer    of int                    (* ':' *)
		 | Bulk       of (string list) option   (* '$' *)
	         | MultiBulk  of (reply list) option    (* '*' e.g. array *)


    val parse    : reply CharParser.charParser
    val toString : reply -> string
  end =
struct
  open ParserCombinators CharParser

  datatype reply = SingleLine of string                 (* '+' *)
		 | Error      of string                 (* '-' *)
		 | Integer    of int                    (* ':' *)
		 | Bulk       of (string list) option   (* '$' *)
	         | MultiBulk  of (reply list) option    (* '*' e.g. array *)

  infix 2 return wth suchthat return guard when
  infixr 1 || <|>
  infixr 3 &&
  infixr 4 << >>

  (* TODO render in a format suitable to send to redis *)
  fun toString r =
    case r of
  	SingleLine s           => ("SingleLine => " ^ s)
      | (Integer s)            => ("Integer    => " ^ Int.toString s)
      | (Bulk NONE)            => ("Bulk       => NONE")
      | (Bulk (SOME s))        => ("Bulk       => Some [" ^ String.concatWith ", " s ^ "]")
      | (MultiBulk NONE)       => ("MultiBulk  :: NONE")
      | (MultiBulk (SOME s))   => ("MultiBulk\n\t" ^ String.concatWith "\n\t" (List.map toString s))
      | (Error s)              => ("Error      => " ^ s)

  val crlf = string "\r\n"

  val parseInt = repeat1 digit wth valOf o Int.fromString o String.implode


  val bulk = join (string "$" >> parseInt << crlf wth
		    (fn i => repeatn i (not crlf >> anyChar) << crlf wth
				     (* FIX only 1 line... Handle null elements *)
				     (fn xs => Bulk (SOME [(String.implode xs)]))))

  val singleLine = string "+" >> repeat1 (not crlf >> anyChar) << crlf
			   wth (fn xs => SingleLine (String.implode xs))

  val error      = string "-" >> repeat1 (not crlf >> anyChar) << crlf
			  wth (fn xs => Error (String.implode xs))

  val integer    = string ":" >> parseInt << crlf wth (fn xs => Integer xs)

  (* FIX phantom argument in mutually recursive definitions *)
  fun dqmsg _ = singleLine
                <|> integer
                <|> bulk
                <|> multibulk ()
                <|> error
                (* <|> (raise (DisqueException "Input ended unexpectedly") *)

  and multibulk _ = join (string "*" >> parseInt << crlf wth
				 (fn i => repeatn i (dqmsg ()) wth
					    (fn x => MultiBulk (SOME x))))

  val parse = dqmsg () << eos
end

(* Disque message queue connection and query operations *)
signature DISQUE =
sig
    type conn = (INetSock.inet, Socket.active Socket.stream) Socket.sock
    type host = string type port = int

    exception DisqueException of string

    val connect : host -> port -> conn
    val close   : conn -> unit

    val info   : conn -> DisqueParser.reply
    val addjob : conn -> string -> string -> DisqueParser.reply
    val getjob : conn -> string -> int -> DisqueParser.reply

    val ackjob  : conn -> string list -> DisqueParser.reply
    val fastack : conn -> string list -> DisqueParser.reply
    val working : conn -> string -> DisqueParser.reply

end

structure Disque :> DISQUE =
struct

type conn = (INetSock.inet, Socket.active Socket.stream) Socket.sock
type host = string
type port = int

exception DisqueException of string

val crlf = "\r\n";

(* Initiate a connection to Redis server. *)
fun connect host port =
  let val s = INetSock.TCP.socket()
  in
      Socket.Ctl.setREUSEADDR (s, true);
      Socket.Ctl.setKEEPALIVE(s,true);
      Socket.connect (s, INetSock.any port);
      INetSock.TCP.setNODELAY (s,true);
      s
  end

(* Initiate a connection to Redis server. *)
fun close s =
  ( Socket.shutdown (s,Socket.NO_RECVS_OR_SENDS)
  ; Socket.close s
  )

fun writedq(out,str) =
  Socket.sendVec(out, Word8VectorSlice.full(Byte.stringToBytes(str)))

open CharParser;

fun recvdq s =
  let val bytes  = Byte.bytesToString(Socket.recvVec(s, 4096))
      fun is_eol s =
        case Stream.front s of
             Stream.Nil => true
           | Stream.Cons (x, s') => x = #"\n"
      val coordStream = CoordinatedStream.coordinate is_eol (Coord.init "disque") (Stream.fromString bytes)
  in (* print ("Debug Raw: " ^ bytes) ; *)
      case CharParser.parseChars DisqueParser.parse coordStream of
	  Sum.INL e     => raise DisqueException ("Disque: " ^ e ^ " => " ^ bytes)
	| Sum.INR r     => r
  end


fun req conn msg
  = ( writedq(conn, msg ^ crlf)
    ; recvdq conn
    )

fun info conn             = req conn ("INFO " ^ crlf)
fun addjob conn queue job = req conn ("ADDJOB " ^ queue ^ " \"" ^ job ^ "\" 0" ^ crlf)
fun getjob conn queue cnt = req conn ("GETJOB FROM " ^ queue ^ " " ^ crlf)

fun ackjob conn jids  = req conn ("ACKJOB " ^ (String.concatWith " " jids) ^ crlf)
fun fastack conn jids = req conn ("FASTACK " ^ (String.concatWith " " jids) ^ crlf)
fun working conn jid  = req conn ("WORKING " ^ jid ^ crlf)

end
