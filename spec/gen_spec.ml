open StdLabels
open Printf

let indent = ref 0
let in_comment = ref false
let emit_location = ref false

let option_iter ~f = function
  | Some v -> f v
  | None -> ()

let emit_loc loc =
  match !emit_location with
  | true ->
    let indent = String.make (!indent * 2) ' ' in
    printf "%s(* %s:%d *)\n" indent __FILE__ loc
  | false ->
    printf "# %d \"%s\"\n" loc __FILE__

let emit ?disable ?loc fmt =
  let loc = match loc with
    | Some loc -> Some loc
    | None when not !emit_location && not !in_comment && false -> begin
        let open Printexc in
        let bt = get_callstack 3 in
        let slot = get_raw_backtrace_slot bt 2 |> convert_raw_backtrace_slot in
        match Slot.location slot with
        | Some slot -> Some slot.line_number
        | None -> None
      end
    | None -> None
  in
  assert (!indent >= 0);
  let indent = String.make (!indent * 2) ' ' in
  (* Get last location *)
  ksprintf (fun s -> match disable with Some true -> () | _ -> option_iter ~f:emit_loc loc; print_string s; ) ("%s" ^^ fmt ^^ "\n") indent

let strip_doc doc =
  String.split_on_char ~sep:'\n' doc
  |> List.rev_map ~f:String.trim
  |> (function "" :: xs -> xs | xs -> xs)
  |> List.rev
  |> (function "" :: xs -> xs | xs -> xs)
  |> List.map ~f:(Str.global_replace (Str.regexp " [ ]*") " ")

let emit_doc = function
  | Some (doc : string) ->
    (* Strip the doc-string *)
    let doc_strings =
      strip_doc doc
      |> List.rev
      |> (function [] -> []
                 | x :: xs -> (x ^ " *)") :: xs)
      |> List.rev
    in
    begin match doc_strings with
    | [] -> ()
    | first :: rest ->
      in_comment := true;
      emit "(** %s" first;
      List.iter ~f:(emit "    %s") rest;
      in_comment := false;
    end
  | None -> ()


module Field = struct
  type t = { name: string; tpe: string; reserved: bool; doc: string option }
end
module Constant = struct
  type t = { name: string; value: int; doc: string option }
end
module Domain = struct
  type t = { name: string; amqp_type: string; doc: string option }
end
module Method = struct
  type t = { name: string; arguments: Field.t list;
             response: string list; content: bool;
             index: int; synchronous: bool; server: bool; client: bool;
             doc: string option
           }
end
module Class = struct
  type t = { name: string; content: Field.t list; index: int;
             methods: Method.t list; doc: string option }
end

type elem =
  | Constant of Constant.t
  | Domain of Domain.t
  | Class of Class.t

let doc xml =
  try
    Ezxmlm.member "doc" xml
    |> Ezxmlm.data_to_string
    |> (fun x -> Some x)
  with
  | Ezxmlm.Tag_not_found _ -> None

let parse_field (attrs, nodes) =
  (* Only look at the attributes *)
  ignore nodes;
  let name =
    match Ezxmlm.get_attr "name" attrs with
    | "type" -> "amqp_type"
    | name -> name
  in
  let tpe =
    match Ezxmlm.get_attr "domain" attrs with
    | d -> d
    | exception Not_found -> Ezxmlm.get_attr "type" attrs
  in

  let reserved = Ezxmlm.mem_attr "reserved" "1" attrs in
  { Field.name; tpe; reserved; doc = doc nodes }

let parse_constant (attrs, nodes) =
  let name = Ezxmlm.get_attr "name" attrs in
  let value = Ezxmlm.get_attr "value" attrs |> int_of_string in
  Constant { Constant.name; value; doc = doc nodes }

let parse_domain (attrs, nodes) =
  ignore nodes;
  let name = Ezxmlm.get_attr "name" attrs in
  let amqp_type = Ezxmlm.get_attr "type" attrs in
  Domain { Domain.name; amqp_type; doc = doc nodes}

let parse_method (attrs, nodes) =
  let name = Ezxmlm.get_attr "name" attrs in
  incr indent;
  let index = Ezxmlm.get_attr "index" attrs |> int_of_string in
  let response =
    Ezxmlm.members_with_attr "response" nodes
    |> List.map ~f:(fun (attrs, _) -> Ezxmlm.get_attr "name" attrs)
  in

  let synchronous =
    match Ezxmlm.get_attr "synchronous" attrs with
    | "1" -> true
    | _ -> false
    | exception Not_found -> false
  in
  let content =
    match Ezxmlm.get_attr "content" attrs with
    | "1" -> true
    | _ -> false
    | exception Not_found -> false
  in
  let arguments = Ezxmlm.members_with_attr "field" nodes |> List.map ~f:parse_field in

  let chassis =
    Ezxmlm.members_with_attr "chassis" nodes
    |> List.map ~f:(fun (attrs, _) -> Ezxmlm.get_attr "name" attrs)
  in
  let client = List.mem "client" ~set:chassis in
  let server = List.mem "server" ~set:chassis in
  decr indent;
  { Method.name; arguments; response; content; index; synchronous;
    client; server; doc = doc nodes }

let parse_class (attrs, nodes) =
  (* All field nodes goes into content *)
  let name = Ezxmlm.get_attr "name" attrs in
  incr indent;
  let index = Ezxmlm.get_attr "index" attrs |> int_of_string in
  let fields = Ezxmlm.members_with_attr "field" nodes |> List.map ~f:parse_field in
  let methods = Ezxmlm.members_with_attr "method" nodes |> List.map ~f:parse_method in
  decr indent;
  Class { Class.name; index; content=fields; methods; doc = doc nodes }

let parse = function
  | `Data _ -> None
  | `El (((_, "constant"), attrs), nodes) -> Some (parse_constant (attrs, nodes))
  | `El (((_, "domain"), attrs), nodes) -> Some (parse_domain (attrs, nodes))
  | `El (((_, "class"), attrs), nodes) -> Some (parse_class (attrs, nodes))
  | `El (((_, name), _), _) -> failwith ("Unknown type: " ^ name)

let parse_amqp xml =
  Ezxmlm.member "amqp" xml
  |> List.map ~f:parse
  |> List.fold_left ~f:(fun acc -> function None -> acc | Some v -> v :: acc) ~init:[]
  |> List.rev

let bind_name str =
  String.map ~f:(function '-' -> '_' | c -> Char.lowercase_ascii c) str

let variant_name str =
  bind_name str
  |> String.capitalize_ascii

(* Remove domains *)
let emit_domains tree =
  let domains = Hashtbl.create 0 in
  List.iter ~f:(function
      | Domain {Domain.name; amqp_type; doc} when name <> amqp_type ->
        Hashtbl.add domains name (amqp_type, doc)
      | _ -> ()
    ) tree;

  emit "(* Domains *)";
  Hashtbl.iter (fun d (t, doc) ->
    emit "";
    emit_doc doc;
    emit ~loc:__LINE__ "type %s = %s" (bind_name d) (bind_name t);
  ) domains;

  emit "";
  emit "(* Aliases *)";
  Hashtbl.iter (fun d (t, _) ->
      emit "let %s = %s" (bind_name d) (variant_name t);
    ) domains;
  emit "";

  (* Alter the tree *)
  let replace lst =
    let open Field in
    List.map ~f:(fun t ->
        let tpe = match Hashtbl.mem domains t.tpe with
          | true -> bind_name t.tpe
          | false -> variant_name t.tpe
        in
        { t with tpe }
      ) lst
  in
  let map = function
    | Domain _ -> None
    | Constant c -> Some (Constant c)
    | Class ({ Class.content; methods; _ } as c) ->
        let methods =
          List.map ~f:(function {Method.arguments; _ } as m ->
              { m with Method.arguments = replace arguments }
            ) methods
        in
        Some (Class { c with Class.methods; content = replace content })
  in
  List.fold_left ~f:(fun acc e -> match map e with Some x -> x :: acc | None -> acc) ~init:[] tree

let emit_constants tree =
  emit "(* Constants *)";
  List.iter ~f:(function Constant { Constant.name; value; doc } ->
    emit "";
    emit_doc doc;
    emit ~loc:__LINE__ "let %s = %d" (bind_name name) value | _ -> ()
  ) tree

let spec_str arguments =
  arguments
  |> List.map ~f:(fun t -> t.Field.tpe)
  |> fun a -> List.append a ["[]"]
  |> String.concat ~sep:" :: "

let emit_method ?(is_content=false) class_index
    { Method.name;
      arguments;
      response;
      content;
      index;
      synchronous;
      client;
      server;
      doc;
    } =

  emit "";
  emit_doc doc;
  emit ~loc:__LINE__ "module %s = struct" (variant_name name);
  incr indent;
  let t_args =
    arguments
    |> List.filter ~f:(fun t -> not t.Field.reserved)
  in
  let option = if is_content then " option" else "" in
  let types = List.map ~f:(fun t -> (bind_name t.Field.name), (bind_name t.Field.tpe) ^ option, t.Field.doc) t_args in

  let t_args = match types with
    | [] -> "()"
    | t -> List.map ~f:(fun (a, _, _) -> a) t |> String.concat ~sep:"; " |> sprintf "{ %s }"
  in
  let names =
    arguments
    |> List.map ~f:(function t when t.Field.reserved -> "_" | t -> bind_name t.Field.name)
  in
  let values =
    arguments
    |> List.map ~f:(function
        | t when t.Field.reserved ->
          "(reserved_value " ^ t.Field.tpe ^ ")"
        | t -> bind_name t.Field.name
      )
    |> String.concat ~sep:" "
  in

  (match types with
   | [] -> emit ~loc:__LINE__ "type t = unit"
   | t ->
     emit ~loc:__LINE__ "type t = {";
     incr indent;
     List.iter ~f:(fun (a, b, doc) ->
       emit "%s: %s;" a b;
       emit_doc doc
     ) t;
     decr indent;
     emit "}");
  emit "";

  if is_content then
    emit ~loc:__LINE__ "open Protocol.Content"
  else
    emit ~loc:__LINE__ "open Protocol.Spec";

  let message_id = sprintf " Types.{ class_id = %d; method_id = %d }" class_index index in
  let spec = spec_str arguments in
  let make =
    let lhs = match names with
      | [] -> ""
      | names -> sprintf "fun %s ->" (String.concat ~sep:" " names)
    in
    sprintf "%s %s" lhs t_args
  in

  let inames = List.filter ~f:((<>) "_") names in
  let make_named = match types, is_content with
    | [], _ -> sprintf "fun f -> f"
    | _ ->
      let access = match is_content with true -> "?" | false -> "~" in
      sprintf "fun f %s () -> f %s" (List.map ~f:(fun n -> access ^ n) inames |> String.concat ~sep:" ") t_args
  in

  let apply = sprintf "fun f %s -> f %s" t_args values in

  let apply_named = match types, is_content with
    | [], _ -> sprintf "fun f -> f"
    | _, true ->
      sprintf "fun f %s -> f %s ()" t_args (List.map ~f:(fun n -> "?" ^ n) inames |> String.concat ~sep:" ")
    | _, false ->
      sprintf "fun f %s -> f %s ()" t_args (List.map ~f:(fun n -> "~" ^ n) inames |> String.concat ~sep:" ")
  in

  emit_loc __LINE__;
  emit "let message_id = %s" message_id;

  let emit_record_fields () =
    incr indent;
    emit "message_id;";
    emit "spec = %s;" spec;
    emit "make = (%s);" make;
    emit "make_named = (%s);" make_named;
    emit "apply = (%s);" apply;
    emit "apply_named = (%s);" apply_named;
    decr indent;
    ()
  in
  emit "let def : _ def = {";
  emit_record_fields ();
  emit "}";
  emit "";

  (* Construct expect functions *)
  let () = match is_content, content with
    | false, false -> emit "let expect f = Service.expect_method def f"
    | false, true -> emit "let expect f = Service.expect_method_content def Content.def f"
    | true, _ -> ()
  in
  (* Emit call definitions *)
  let response = List.map ~f:variant_name response in
  if ((synchronous && response != []) || not synchronous) then begin
    let id r = match response with
      | [] | [_]-> "(fun id -> id)"
      | _ -> "(fun m -> `" ^ r ^ " m)"
    in
    if client && not content then begin
      emit ~loc:__LINE__ "let reply = (%s)"
        ("def" :: (response |> List.map ~f:(Printf.sprintf "%s.def")) |> String.concat ~sep:", ");

      match response with
      | [] -> emit ~loc:__LINE__ "let server_request = Service.server_request def"
      | [rep] -> emit ~loc:__LINE__ "let server_request_reply = Service.server_request_response def %s.def" rep
      | _ -> failwith "Multiple server replies not expected"
    end;

    if server then begin
      emit ~loc:__LINE__ "let request = (%s)"
        ("def" :: (response |> List.map ~f:(Printf.sprintf "%s.def")) |> String.concat ~sep:", ");

      let responses = List.map ~f:(fun response -> sprintf "(%s.expect %s)" response (id response)) response in
      emit ~loc:__LINE__ "let client_request = Service.client_request_response def [%s]"
        (String.concat ~sep:"; " responses)
    end;
  end;
  decr indent;
  emit "end";
  ()


let emit_class { Class.name; content; index; methods; doc } =
  (* Reorder modules based on dependencies *)
  let rec reorder methods =
    let rec move_down = function
      | { Method.response; _} as m :: x :: xs when
          List.exists ~f:(fun r -> List.exists ~f:(fun {Method.name; _} -> name = r) (x :: xs)) response -> x :: move_down (m :: xs)
      | x :: xs -> x :: move_down xs
      | [] -> []
    in
    let ms = move_down methods in
    if ms = methods then ms
    else reorder ms
  in
  let methods = reorder methods in
  emit "";
  emit_doc doc;
  emit ~loc:__LINE__ "module %s = struct" (variant_name name);
  incr indent;

  if (content != []) then
    emit_method ~is_content:true
      index { Method.name = "content";
              arguments = content;
              response = [];
              content = false;
              index = 0; (* must be zero *)
              synchronous = false;
              server=false;
              client=false;
              doc = None;
            };

  List.iter ~f:(emit_method index) methods;

  decr indent;
  emit "end";
  ()

let emit_specification tree =
  emit_loc __LINE__;
  (* emit "open Amqp_client_lib"; *)
  emit "open Types";
  emit "open Protocol";
  emit "";
  emit_domains tree
  |> List.iter ~f:(function Class x -> emit_class x | _ -> ());
  (* emit_printer tree; *)
  ()

type output = Constants | Specification

let () =
  (* Argument parsing *)
  let output_type = ref Specification in
  let filename = ref "" in

  Arg.parse
    ["-type", Arg.Symbol (["constants"; "specification"],
                          fun t -> output_type := match t with
                            | "constants" -> Constants
                            | "specification" -> Specification
                            | _ -> failwith "Illegal argument"
                         ), "Type of output";
     "-noloc", Arg.Clear emit_location, "Inhibit emission of location pointers"
    ]
    (fun f -> filename := f)
    "Generate protocol code";

  let xml =
    let in_ch = open_in !filename in
    let (_, xml) = Ezxmlm.from_channel in_ch in
    close_in in_ch;
    xml
  in
  let tree = xml |> parse_amqp in
  emit "(** Internal - Low level protocol description *)";
  emit "(***********************************)";
  emit "(* AUTOGENERATED FILE: DO NOT EDIT *)";
  emit "(* %s %s %s %s *)" Sys.argv.(0) Sys.argv.(1) Sys.argv.(2) Sys.argv.(3);
  emit "(***********************************)";
  emit "";

  begin
    match !output_type with
    | Constants ->
      emit_constants tree
    | Specification ->
      emit_specification tree
  end;
  assert (!indent = 0);
  ()
