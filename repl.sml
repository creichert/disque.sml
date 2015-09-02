
CM.autoload "disque.cm";
CM.autoload "cmlib/cmlib.cm";
CM.autoload "parcom/parcom.cm";

open TextIO;
open INetSock;
open Sum;

open DisqueParser;
open Disque;

(************************************************************************)
(* Examples and tests                                                   *)
(************************************************************************)

(* Example parsing *)
fun testParser _ =
    let
	val str = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
	val str1 = "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"
        val str2 = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        val str3 = "*2\r\n$10\r\nfo\no o ooo\r\n$3\r\nbar\r\n"
        val str4 = "-lksjdf;sldk\r\n"
	fun is_eol s =
	  case Stream.front s of
	      Stream.Nil => true
	    | Stream.Cons (x, s') => x = #"\n"
	val coordStream = CoordinatedStream.coordinate is_eol (Coord.init "Disque test 0") (Stream.fromString str)
	val coordStream1 = CoordinatedStream.coordinate is_eol (Coord.init "Disque test 1") (Stream.fromString str1)
	val coordStream2 = CoordinatedStream.coordinate is_eol (Coord.init "Disque test 2") (Stream.fromString str2)
	val coordStream3 = CoordinatedStream.coordinate is_eol (Coord.init "Disque test 3") (Stream.fromString str3)
	val coordStream4 = CoordinatedStream.coordinate is_eol (Coord.init "Disque test 4") (Stream.fromString str4)

	(* Test 1 *)
	val tst = case CharParser.parseChars DisqueParser.parse coordStream of
		      Sum.INL e  => raise Fail (e ^ " on input =>\n" ^ str)
		    | Sum.INR r  => DisqueParser.toString r
	(*
        val tst1 = case CharParser.parseChars DisqueParser.parse coordStream1 of
		       Sum.INL e  => raise Fail (e ^ " on input =>\n" ^ str)
		     | Sum.INR r  => DisqueParser.toString r *)

	val tst2 = case CharParser.parseChars DisqueParser.parse coordStream2 of
		       Sum.INL e  => raise Fail (e ^ " on input =>\n" ^ str)
		     | Sum.INR r  => DisqueParser.toString r

	val tst3 = case CharParser.parseChars DisqueParser.parse coordStream3 of
		       Sum.INL e  => raise Fail (e ^ " on input =>\n" ^ str)
		     | Sum.INR r  => DisqueParser.toString r

	val tst4 = case CharParser.parseChars DisqueParser.parse coordStream4 of
		       Sum.INL e  => raise Fail (e ^ " on input =>\n" ^ str)
		     | Sum.INR r  => DisqueParser.toString r

    in  print ("\nDisqueParser tests\n\n")

      ; print ("Test 0: *5\\r\\n:1\\r\\n:2\\r\\n:3\\r\\n:4\\r\\n$6\\r\\nfoobar\\r\\n\r\n")
      ; print (tst ^ "\n\n")
        (* ; print ("Test 1: " ^ tst1) *)
      ; print ("Test 2: *2\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n\r\n")
      ; print (tst2 ^ "\n\n")

      ; print ("Test 3: *2\\r\\n$4\\r\\nfo\\no\\r\\n$3\\r\\nbar\\r\\n\r\n")
      ; print (tst3 ^ "\n\n")

      ; print ("Test 4:\n")
      ; print (tst4 ^ "\n\n")

end

(* example entrypoint *)
fun disque (args) =
  let val s = Disque.connect "127.0.0.1" 7711
      val _ = Disque.addjob s "push" "{job:2}"
      val _ = Disque.addjob s "push" "{job:5}"
      val _ = Disque.addjob s "push" "{job:fi iiiiiii  \\n iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii}"
      val j = Disque.getjob s "push" 0
      val j2 = Disque.getjob s "push" 0
      val j3 = Disque.getjob s "push" 0
      (* val j2 = Disque.getjob s "otherqueue" 0 *)
      (* val a = Disque.ackjob s ["jid1", "jid2"] *)
  in
      print("Job 1:\n" ^ toString j3 ^ "\n\n\n")
    ; print("Job 2:\n" ^ toString j2 ^ "\n\n\n")
    ; print("Job 3:\n" ^ toString j3 ^ "\n\n\n")
    ; Disque.close(s)
  end handle
        (OS.SysErr (s, SOME e))		=> log.error ("\nSystem Error " ^ s ^  "\n\n")
      | (Fail s)			=> log.error ("\nFail " ^ s ^  "\n\n")
      | (Disque.DisqueException s)      => log.error ("\n(error) " ^ s ^  "\n\n")
      | exn				=> log.error ("\n(error) Uknown: " ^ exnMessage exn ^ "\n\n")

val _ = print "\n\n(***************************************************************************)\n"
val _ = print "(***************************************************************************)\n\n"

val args = CommandLine.arguments();
val _ =   print "\n\n(***************************************************************************)"
        ; print "\n\n------> Disque test\n\n"
        ; disque(args) ;

val _ = print "\n\n(***************************************************************************)\n"
val _ = print "(***************************************************************************)\n\n"

val _ = print "Try running `- testParser()'\n"
