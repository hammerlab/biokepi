
open Nonstd
module K = Ketrew.EDSL

let say fmt = ksprintf (printf "%s\n%!") fmt

let () =
  say "Hello World"
