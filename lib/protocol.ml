(** Internal *)

open StdLabels
open Types
type nonrec int = int

type _ elem =
  | Bit: bool elem
  | Octet: int elem
  | Short: int elem
  | Long: int elem
  | Longlong: int elem
  | Shortstr: string elem
  | Longstr: string elem
  | Float: float elem
  | Double: float elem
  | Decimal: decimal elem
  | Table: table elem
  | Timestamp: timestamp elem
  | Array: array elem
  | Unit: unit elem

let tap a b = a b; b

let reserved_value: type a. a elem -> a = function
  | Bit -> false
  | Octet -> 0
  | Short -> 0
  | Long -> 0
  | Longlong -> 0
  | Shortstr -> ""
  | Longstr -> ""
  | Float -> 0.0
  | Double -> 0.0
  | Decimal -> { digits = 0; value = 0 }
  | Table -> []
  | Timestamp -> 0
  | Array -> []
  | Unit -> ()

module Be = Cstruct.BE
let rec decode_bit t off = Cstruct.get_uint8 t off = 1, off + 1
and decode_octet t off = Cstruct.get_uint8 t off, off + 1
and decode_short t off = Be.get_uint16 t off, off + 2
and decode_long t off = Be.get_uint32 t off |> Int32.to_int, off + 4
and decode_longlong t off = Be.get_uint64 t off |> Int64.to_int, off + 8
and decode_shortstr t off =
  let len, off = decode_octet t off in
  Cstruct.to_string ~off ~len t, (off + len)
and decode_longstr t off =
  let len, off = decode_long t off in
  Cstruct.to_string ~off ~len t, (off + len)
and decode_table t off =
  let size, off = decode_long t off in
  let end_off = size + off in
  let rec read_elem = function
    | off when off = end_off -> []
    | off when off > end_off -> failwith "Table read past offset"
    | off ->
      let key, off = decode_shortstr t off in
      let value, off = decode_field t off in
      (key, value) :: read_elem off
  in
  read_elem off, end_off
and decode_timestamp t off = decode_longlong t off
and decode_float t off =
  Be.get_uint32 t off |> Int32.float_of_bits, off + 4
and decode_double t off =
  Be.get_uint64 t off |> Int64.float_of_bits, off + 8
and decode_decimal t off =
  let digits, off = decode_octet t off in
  let value, off = decode_long t off in
  { digits; value }, off
and decode_array t off =
  let size, off = decode_long t off in
  let end_off = off + size in
  let rec read_array = function
    | off when off = end_off -> []
    | off when off > end_off -> failwith "Read past array"
    | off ->
      let v, off = decode_field t off in
      v :: read_array off
  in
  read_array off, end_off
and decode_unit _t off = (), off
and decode: type a. a elem -> Cstruct.t -> int -> a * int = function
  | Bit -> decode_bit
  | Octet -> decode_octet
  | Short -> decode_short
  | Long -> decode_long
  | Longlong -> decode_longlong
  | Shortstr -> decode_shortstr
  | Longstr -> decode_longstr
  | Table -> decode_table
  | Timestamp -> decode_timestamp
  | Float -> decode_float
  | Double -> decode_double
  | Decimal -> decode_decimal
  | Array -> decode_array
  | Unit -> decode_unit
and decode_field t off =
  let v, off = decode_octet t off in
  match Char.chr v with
  | 't' -> let v, off = decode_bit t off in VBoolean v, off
  | 'b' | 'B' -> let v, off = decode_octet t off in VShortshort v, off
  | 'u' | 'U' -> let v, off = decode_short t off in VShort v, off
  | 'i' | 'I' -> let v, off = decode_long t off in VLong v, off
  | 'l' | 'L' -> let v, off = decode_longlong t off in VLonglong v, off
  | 'f' -> let v, off = decode_float t off in VFloat v, off
  | 'd' -> let v, off = decode_double t off in VDouble v, off
  | 'D' -> let v, off = decode_decimal t off in VDecimal v, off
  | 's' -> let v, off = decode_shortstr t off in VShortstr v, off
  | 'S' -> let v, off = decode_longstr t off in VLongstr v, off
  | 'A' -> let v, off = decode_array t off in VArray v, off
  | 'T' -> let v, off = decode_timestamp t off in VTimestamp v, off
  | 'F' -> let v, off = decode_table t off in VTable v, off
  | 'V' -> let v, off = decode_unit t off in VUnit v, off
  | _ -> failwith "Uknown table value"

let rec encode_bit t off v = Cstruct.set_uint8 t off (if v then 1 else 0); off + 1
and encode_octet t off v = Cstruct.set_uint8 t off v; off + 1
and encode_short t off v = Be.set_uint16 t off v; off + 2
and encode_long t off v = Be.set_uint32 t off (Int32.of_int v); off + 4
and encode_longlong t off v = Be.set_uint64 t off (Int64.of_int v); off + 8
and encode_shortstr t off v =
  let len = String.length v in
  let off = encode_octet t off len in
  Cstruct.blit_from_string v 0 t off len;
  off + len
and encode_longstr t off v =
  let len = String.length v in
  let off = encode_long t off len in
  Cstruct.blit_from_string v 0 t off len;
  off + len
and encode_table t off v =
  (* Make room for the size, and remember the offset *)
  let size_off = off in
  let off = encode_long t off 0 in
  let start_off = off in
  let off =
    List.fold_left ~init:off ~f:(fun off (k, v) ->
      let off = encode_shortstr t off k in
      let off = encode_field t off v in
      off
    ) v
  in
  (* Rewrite the length of the table *)
  let _: int = encode_long t size_off (off - start_off) in
  off
and encode_timestamp t off v = encode_longlong t off v
and encode_float t off v = Be.set_uint32 t off (Int32.bits_of_float v); off + 4
and encode_double t off v = Be.set_uint64 t off (Int64.bits_of_float v); off + 8
and encode_decimal t off { digits; value } =
  let off = encode_octet t off digits in
  let off = encode_long t off value in
  off
and encode_array t off v =
  let size_off = off in
  let off = encode_long t off 0 in
  let start_off = off in
  let off = List.fold_left ~init:off ~f:(encode_field t) v in
  (* Rewrite the length of the table *)
  let _: int = encode_long t size_off (off - start_off) in
  off
and encode_unit _t off () = off
and encode: type a. a elem -> Cstruct.t -> int -> a -> int = function
  | Bit -> encode_bit
  | Octet -> encode_octet
  | Short -> encode_short
  | Long -> encode_long
  | Longlong -> encode_longlong
  | Shortstr -> encode_shortstr
  | Longstr -> encode_longstr
  | Table -> encode_table
  | Timestamp -> encode_timestamp
  | Float -> encode_float
  | Double -> encode_double
  | Decimal -> encode_decimal
  | Array -> encode_array
  | Unit -> encode_unit
and encode_field =
  fun t off ->
  let encode_field c encode_v t off v =
    let off = encode_octet t off (Char.code c) in
    encode_v t off v
  in
  function
  | VBoolean v -> encode_field 't' encode_bit t off v
  | VShortshort v -> encode_field 'b' encode_octet t off v
  | VShort v -> encode_field 'u' encode_octet t off v
  | VLong v -> encode_field 'i' encode_long t off v
  | VLonglong v -> encode_field 'l' encode_longlong t off v
  | VShortstr v -> encode_field 's' encode_shortstr t off v
  | VLongstr v -> encode_field 'S' encode_longstr t off v
  | VFloat v -> encode_field 'f' encode_float t off v
  | VDouble v -> encode_field 'd' encode_double t off v
  | VDecimal v -> encode_field 'D' encode_decimal t off v
  | VTable v -> encode_field 'F' encode_table t off v
  | VArray v -> encode_field 'A' encode_array t off v
  | VTimestamp v -> encode_field 'T' encode_timestamp t off v
  | VUnit () -> encode_field 'V' encode_unit t off ()

let rec length_bit (_: bool) = 1
and length_octet (_: int) = 1
and length_short (_: int) = 2
and length_long (_: int) = 4
and length_longlong (_ : int) = 8
and length_shortstr v = length_octet 0 + String.length v
and length_longstr v = length_long 0 + String.length v
and length_table v =
  List.fold_left ~init:(length_long 0) ~f:(fun length (k, v) ->
    length + length_shortstr k + length_field v
  ) v
and length_timestamp v = length_longlong v
and length_float (_ : float) = length_long 0
and length_double (_ : float) = length_longlong 0
and length_decimal (_ : decimal) =
  length_octet 0 + length_long 0
and length_array v =
  List.fold_left ~init:(length_long 0) ~f:(fun length v ->
    length + length_field v
  ) v
and length_unit () = 0
and length: type a. a elem -> a -> int = function
  | Bit -> length_bit
  | Octet -> length_octet
  | Short -> length_short
  | Long -> length_long
  | Longlong -> length_longlong
  | Shortstr -> length_shortstr
  | Longstr -> length_longstr
  | Table -> length_table
  | Timestamp -> length_timestamp
  | Float -> length_float
  | Double -> length_double
  | Decimal -> length_decimal
  | Array -> length_array
  | Unit -> length_unit
and length_field =
  let length_field length_v v = length_octet 0 + length_v v in
  function
  | VBoolean v -> length_field length_bit v
  | VShortshort v -> length_field length_octet v
  | VShort v -> length_field length_octet v
  | VLong v -> length_field length_long v
  | VLonglong v -> length_field length_longlong v
  | VShortstr v -> length_field length_shortstr v
  | VLongstr v -> length_field length_longstr v
  | VFloat v -> length_field length_float v
  | VDouble v -> length_field length_double v
  | VDecimal v -> length_field length_decimal v
  | VTable v -> length_field length_table v
  | VArray v -> length_field length_array v
  | VTimestamp v -> length_field length_timestamp v
  | VUnit () -> length_field length_unit ()


let elem_to_string: type a. a elem -> string = function
  | Bit -> "Bit"
  | Octet -> "Octet"
  | Short -> "Short"
  | Long -> "Long"
  | Longlong -> "Longlong"
  | Shortstr -> "Shortstr"
  | Longstr -> "Longstr"
  | Table -> "Table"
  | Timestamp -> "Timestamp"
  | _ -> "Unknown"

module Spec = struct
  type (_, _) spec =
    | [] : ('a, 'a) spec
    | (::)  : 'a elem * ('b, 'c) spec -> (('a -> 'b), 'c) spec

  type ('t, 'input, 'output, 'make, 'make_named, 'make_named_result, 'apply, 'apply_result, 'apply_named, 'apply_named_result) def = {
    message_id: message_id;
    spec: ('input, 'output) spec;
    make: 'make; (* a -> b -> t *)
    make_named: ('t -> 'make_named_result) -> 'make_named;
    apply: 'apply -> 't -> 'apply_result; (* (a -> b -> c) -> t -> c *)
    apply_named: 'apply_named -> 't -> 'apply_named_result;
  }

  type (_, _) edef = Def: ('t, _, 'output, _, _, _, _, _, _, _) def -> ('t, 'output) edef

  let rec read: type b c. (b, c) spec -> b -> Cstruct.t -> int -> c = function
    | (Bit :: _) as spec ->
      let reader = read_bits 8 spec in
      fun b t off ->
        let v, off = decode_octet t off in
        reader b v t off
    | head :: tail ->
      let reader = read tail in
      let decoder = decode head in
      fun b t off ->
        let v, off = decoder t off in
        reader (b v) t off
    | [] ->
      fun b _t _off  -> b

  and read_bits: type b c. int -> (b, c) spec -> b -> int -> Cstruct.t -> int -> c = fun bits -> function
    | Bit :: tail when bits > 0 ->
      let reader = read_bits (bits - 1) tail in
      fun b acc t off ->
        let b' = b (acc mod 2 = 1) in
        reader b' (acc/2) t off
    | tail ->
      let reader = read tail in
      fun b _acc t off -> reader b t off

  let rec write: type b. (b, int) spec -> Cstruct.t -> int -> b = function
    | (Bit :: _) as spec ->
      write_bits 8 spec 0
    | spec :: tail ->
      let encoder = encode spec in
      let writer = write tail in
      fun t off f ->
        let off = encoder t off f in
        writer t off
    | [] -> fun _ off -> off

  and write_bits: type b. int -> (b, int) spec -> int -> Cstruct.t -> int -> b = fun c -> function
    | Bit :: tail when c > 0 ->
      let writer = write_bits (c-1) tail in
      fun acc t off v ->
        let acc = match v with
          | false -> acc
          | true -> acc lor (1 lsl (8-c))
        in
        writer acc t off
    | spec ->
      let writer = write spec in
      fun acc t off ->
        let off = encode_octet t off acc in
        writer t off


  let rec write_apply: type a b. (b, a) spec -> Cstruct.t -> int -> (Cstruct.t -> a) -> b = function
    | (Bit :: _) as spec ->
      write_bits_apply 8 spec 0
    | spec :: tail ->
      let encoder = encode spec in
      let writer = write_apply tail in
      fun t off f v ->
        let off = encoder t off v in
        writer t off f
    | [] -> fun t _off f -> f t

  and write_bits_apply: type a b. int -> (b, a) spec -> int -> Cstruct.t -> int -> (Cstruct.t -> a) -> b = fun c -> function
    | Bit :: tail when c > 0 ->
      let writer = write_bits_apply (c-1) tail in
      fun acc t off f v ->
        let acc = match v with
          | false -> acc
          | true -> acc lor (1 lsl (8-c))
        in
        writer acc t off f
    | spec ->
      let writer = write_apply spec in
      fun acc t off f ->
        let off = encode_octet t off acc in
        writer t off f

  let rec size: type b. (b, int) spec -> int -> b = function
    | (Bit :: _) as spec ->
      size_bits 8 spec
    | spec :: tail ->
      let sizer = size tail in
      let size = length spec in
      fun acc v ->
        sizer (acc + size v)
    | [] -> fun acc -> acc

  and size_bits: type b. int -> (b, int) spec -> int -> b = fun c -> function
    | Bit :: tail when c > 0 ->
      let sizer = size_bits (c-1) tail in
      fun acc _ -> sizer acc
    | spec ->
      fun acc -> size spec (length_octet 0 + acc)

  let size spec = size spec 0

  let rec to_string: type a b. (a, b) spec -> string = function
    | x :: xs -> elem_to_string x ^ " :: " ^ to_string xs
    | [] -> "[]"
end

module Content = struct
  type (_, _) spec =
    | [] : ('a, 'a) spec
    | (::)  : 'a elem * ('b, 'c) spec -> (('a option -> 'b), 'c) spec

  type ('t, 'input, 'output, 'make, 'make_named, 'make_named_result, 'apply, 'apply_result, 'apply_named, 'apply_named_result) def = {
    message_id: message_id;
    spec: ('input, 'output) spec;
    make: 'make; (* a -> b -> t *)
    make_named: ('t -> 'make_named_result) -> 'make_named;
    apply: 'apply -> 't -> 'apply_result; (* (a -> b -> c) -> t -> c *)
    apply_named: 'apply_named -> 't -> 'apply_named_result;
  }

  type (_, _) edef = Def: ('t, _, 'output, _, _, _, _, _, _, _) def -> ('t, 'output) edef

  let rec elements: type a b. (a, b) spec -> int = function
    | _ :: tail -> 1 + elements tail
    | [] -> 0

  (* This is not following spec. There may be indefinite content
     flags, but the current implementation only supports up to 15 flags.
  *)
  let rec read: type b c. (b, c) spec -> b -> int -> Cstruct.t -> int -> c = function
  | Bit :: tail ->
    let reader = read tail in
    fun b flags t off ->
      let value = if (flags mod 2 = 1) then Some true else Some false in
      reader (b value) (flags lsr 1) t off
  | head :: tail ->
    let reader = read tail
    and decoder = decode head in
    fun b flags t off ->
      let value, off =
        if flags mod 2 = 1 then
          let v, off = decoder t off in
          Some v, off
        else
          None, off
      in
      reader (b value) (flags lsr 1) t off
  | [] ->
    fun b _flags _t _off -> b

  let rec write: type b. (b, int) spec -> int -> Cstruct.t -> int -> b = function
  | Bit :: tail ->
    let writer = write tail in
    fun flags t off v ->
      let flags = flags * 2 + (match v with Some true -> 1 | _ -> 0) in
      writer flags t off
  | spec :: tail ->
    let encoder = encode spec
    and writer = write tail in
    fun flags t off v ->
      let flags = flags * 2 in
      let flags, off  = match v with
        | Some v ->
          flags + 1, encoder t off v
        | None ->
          flags, off
      in
      writer flags t off
  | [] -> fun flags _t _off -> flags

  let rec size: type b. (b, int) spec -> int -> b = function
  | Bit :: tail ->
    let sizer = size tail in
    fun acc _ -> sizer acc
  | spec :: tail ->
    let sizer = size tail in
    let size = length spec in
    fun acc v ->
      let size = match v with
        | Some v -> size v
        | None -> 0
      in
      sizer (acc + size)
  | [] -> fun acc -> acc
  let size spec = size spec 0
end
