
(** 

   We use here the {!Optimization_framework} module to apply a bit more than
   {{:https://en.wikipedia.org/wiki/Lambda_calculus}Β-reduction}:  we also try
   to apply the [List_repr.map], and build [List_repr.make] values.

   This optimization-pass is a good example of use of the
   {!Optimization_framework}.
*)

open Nonstd

module Apply_optimization_framework
    (Input : Semantics.Bioinformatics_base)
= struct
  (** The “core” of the transformation is implemented here.

      {!Input} is the input language.
      
      {!Transformation_types} defines a GADT that encodes the subset of the
      EDSL that is of interest to the transformation.

      ['a Transformation_types.term] is designed to allow us to pattern match
      (in the function {!Transformation_types.bwd}).

      All the terms of the input language that are not relevant to the
      transformation are wrapped in {!Transformation_types.Unknown}.
  *)

  module Transformation_types = struct
    type +'a from = 'a Input.repr
    type +'a term = 
      | Unknown : 'a from -> 'a term
      | Apply : ('a -> 'b) term * 'a term -> 'b term
      | Lambda : ('a term -> 'b term) -> ('a -> 'b) term
      | List_make: ('a term) list -> 'a list term
      | List_map: ('a list term * ('a -> 'b) term) -> 'b list term
      | Pair: 'a term * 'b term -> ('a * 'b) term
      | Fst:  ('a * 'b) term -> 'a term
      | Snd:  ('a * 'b) term -> 'b term
    let fwd x = Unknown x
    let rec bwd : type a. a term -> a from =
      function
      | Apply (Lambda f, v) -> bwd (f v)
      | Apply (other, v) -> Input.apply (bwd other) (bwd v)
      | List_map (List_make l, Lambda f) -> 
        Input.list (List.map ~f:(fun x -> bwd (f x)) l)
      | List_map (x, f) ->
        Input.list_map ~f:(bwd f) (bwd x)
      | Lambda f ->
        Input.lambda (fun x -> (bwd (f (fwd x))))
      | List_make l -> Input.list (List.map ~f:bwd l)
      | Fst (Pair (a, b)) -> bwd a
      | Snd (Pair (a, b)) -> bwd b
      | Pair (a, b) -> Input.pair (bwd a) (bwd b)
      | Fst b -> Input.pair_first (bwd b)
      | Snd b -> Input.pair_second (bwd b)
      | Unknown x -> x
  end

  (** Applying this functor just adds functions that we don't use yet; so this
      could be simplified in the future. *)
  module Transformation =
    Optimization_framework.Define_transformation(Transformation_types)
  open Transformation

  (** {!Language_delta} is where we “intercept” the terms of the language that
      are interesting.

      For all the other ones {!Transformation_types.fwd} will be used
      by {!Optimization_framework.Generic_optimizer}.

  *)
  module Language_delta = struct
    open Transformation_types
    let apply f x = Apply (f, x)
    let lambda f = Lambda f
    let list l = List_make l
    let list_map l ~f = List_map (l, f)
    let pair a b = Pair (a, b)
    let pair_first p = Fst p
    let pair_second p = Snd p
  end

end

(** {!Apply} is the entry point that transforms EDSL terms.  *)
module Apply (Input : Semantics.Bioinformatics_base) = struct
  module The_pass = Apply_optimization_framework(Input)

  (** We populate the module with the default implementations (which do
      nothing). *)
  include Optimization_framework.Generic_optimizer(The_pass.Transformation)(Input)

  (** We override the few functions we're interested in. *)
  include The_pass.Language_delta
end

