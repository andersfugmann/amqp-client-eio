(** Basic Amqp types *)
type message_id = { class_id: int; method_id: int }
module Message_id = struct
  type t = message_id
  let compare { class_id = a; method_id = b} { class_id = a'; method_id = b'} =
    match Int.compare a a' with
    | 0 -> Int.compare b b'
    | n -> n
  let hash = Hashtbl.hash
  let sexp_of_t _t = failwith "Sexp not implemented"

  let equal a b = compare a b = 0
end

module Frame_type = struct
  type t =
    | Method
    | Content_header
    | Content_body
    | Heartbeat

  let of_int = function
    | n when n = Constants.frame_method -> Method
    | n when n = Constants.frame_header -> Content_header
    | n when n = Constants.frame_body -> Content_body
    | n when n = Constants.frame_heartbeat -> Heartbeat
    | _n -> failwith "Unknown frame type"

  let to_int = function
    | Method -> Constants.frame_method
    | Content_header -> Constants.frame_header
    | Content_body -> Constants.frame_body
    | Heartbeat -> Constants.frame_heartbeat

  let equal = (=)
end

type bit = bool
and octet = int
and short = int
and long = int
and longlong = int
and shortstr = string
and longstr = string
and timestamp = int
and decimal = { digits : int; value: int }
and table = (string * value) list
and array = value list
and value =
  | VBoolean of bool
  | VShortshort of int
  | VShort of int
  | VLong of int
  | VLonglong of int
  | VShortstr of string (* Not accepted by rabbitmq *)
  | VLongstr of string
  | VFloat of float
  | VDouble of float
  | VDecimal of decimal
  | VTable of table
  | VArray of value list
  | VTimestamp of int
  | VUnit of unit

type header = string * value

let rec print_type indent t =
  let open Printf in
  match t with
  | VTable t ->
    let indent' = indent ^ "  " in
    printf "[\n";
    List.iter (fun (k, v) -> printf "%s%s: " indent' k; print_type (indent')  v; printf "\n") t;
    printf "%s]" indent;
  | VBoolean v -> printf "%b" v
  | VShortshort v
  | VShort v
  | VLong v
  | VTimestamp v
  | VLonglong v -> printf "%d" v
  | VShortstr v
  | VLongstr v -> printf "%s" v
  | VFloat v
  | VDouble v-> printf "%f" v
  | VDecimal v -> printf "%f" (float v.value /. float v.digits)
  | VArray a ->
    let indent' = indent ^ "  " in
    printf "[\n";
    List.iter (fun v -> printf "%s" indent'; print_type (indent')  v; printf "\n") a;
    printf "%s]" indent;
  | VUnit _ -> printf "\n"

(* Revert type aliasing for merlin *)
type nonrec int = int
type nonrec string = string
type nonrec bool = bool
