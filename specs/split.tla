------------------------------- MODULE split ---------------------------------
(* Models repeated dynamic range splitting of a keyspace and checks that every
   key is owned by exactly one range at every reachable state — not just
   immediately after a single split, but across an arbitrary sequence of splits
   of arbitrary sub-ranges, which is what actually happens under load. *)

EXTENDS FiniteSets

CONSTANT Keys

VARIABLES ranges  \* a set of non-empty subsets of Keys that partition Keys

Init == ranges = {Keys}

\* Split picks any EXISTING range r and any non-trivial partition of it into
\* left/right, replacing r with the two children. Splitting is not confined to
\* the original whole keyspace -- a child range can itself be split again,
\* which is the actual multi-split scenario the invariant needs to survive.
Split(r, left, right) ==
    /\ r \in ranges
    /\ left \cup right = r
    /\ left \cap right = {}
    /\ left # {}
    /\ right # {}
    /\ ranges' = (ranges \ {r}) \cup {left, right}

Next == \E r \in ranges : \E left \in SUBSET r : \E right \in SUBSET r : Split(r, left, right)

Spec == Init /\ [][Next]_ranges

\* The property that matters operationally: no key is ever unowned or double-owned,
\* across any number of splits of any ranges, in any order.
OwnedExactlyOnce == \A key \in Keys : Cardinality({r \in ranges : key \in r}) = 1

=============================================================================
