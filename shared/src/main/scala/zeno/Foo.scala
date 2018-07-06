trait Foo {
  type T
}

trait Bar[F <: Foo] {
  def yo(a: F#T): Unit
}
