package dev.capslock.z3s

import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{EitherValues, Inside, Inspectors, OptionValues}
import org.scalatest.funsuite.{AnyFunSuite, AsyncFunSuite}
import org.scalatest.matchers.should.Matchers

/** 等価比較は型を検査する`should ===`で書く */
trait SpecMatchers
    extends Matchers
    with TypeCheckedTripleEquals
    with EitherValues
    with OptionValues
    with Inspectors
    with Inside

/** 同期的なテスト */
trait UnitSpec extends AnyFunSuite with SpecMatchers

/** `IO[Assertion]`を返すテスト */
trait IOSpec extends AsyncFunSuite with AsyncIOSpec with SpecMatchers
