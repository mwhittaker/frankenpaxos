package frankenpaxos.mencius

// Regular Mencius has clients, leaders, acceptors, and replicas. Decoupled
// Mencius adds batchers, proxy leaders, and proxy replicas.
//
// To avoid having to implement Mencius _and_ decoupled Mencius, we implement
// only decoupled Mencius. Then, to simulate Mencius, we place different
// components in the same process. For example, imagine we want to run coupled
// Mencius with f = 1. We have the following nodes.
//
//    1     2     3     4     5   6
//  | o | o o o | o | o o o | o | o |
//  | o | o o o | o | o o o | o | o |
//  | o | o o o | o | o o o | o | o |
//
//  (1) Batchers
//  (2) Leaders
//  (3) Proxy Leaders
//  (4) Acceptors
//  (5) Replicas
//  (6) Proxy Replicas
//
// If we want to run these on three machines---A, B, and C---we colocate like
// this:
//
//    1     2     3     4     5   6
//  | A | A B C | A | A B C | A | A |
//  | B | B C A | B | A B C | B | B |
//  | C | C A B | C | A B C | C | C |
sealed trait DistributionScheme
case object Hash extends DistributionScheme
case object Colocated extends DistributionScheme
