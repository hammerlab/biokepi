
(** 

   We use here the {!Optimization_framework} module to apply a big more than
   {{:https://en.wikipedia.org/wiki/Lambda_calculus}Β-reduction}.  We also try
   to apply the [List_repr.map], and build [List_repr.make] values.

*)

open Nonstd

module Apply_optimization_framework
    (Input : Semantics.Bioinformatics_base)
= struct
  module Transformation_types = struct
    type 'a from = 'a Input.repr
    type 'a term = 
      | Unknown : 'a from -> 'a term
      | Apply : ('a -> 'b) term * 'a term -> 'b term
      | Lambda : ('a term -> 'b term) -> ('a -> 'b) term
      | List_make: ('a term) list -> 'a list term
      | List_map: ('a list term * ('a -> 'b) term) -> 'b list term
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
      | Unknown x -> x
  end
  open Transformation_types
  module Transformation =
    Optimization_framework.Define_transformation(Transformation_types)
  open Transformation
  module Language_delta = struct
    let apply f x = Apply (f, x)
    let lambda f = Lambda f
    let list l = List_make l
    let list_map l ~f = List_map (l, f)
  end
end
module Make (Input : Semantics.Bioinformatics_base) = struct
  module The_pass = Apply_optimization_framework(Input)
  include Optimization_framework.Generic_optimizer(The_pass.Transformation)(Input)
  include The_pass.Language_delta
end

