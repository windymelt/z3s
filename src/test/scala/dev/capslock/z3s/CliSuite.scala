package dev.capslock.z3s

import munit.FunSuite

class CliSuite extends FunSuite:
  private def restoreForce(args: String*): Option[Boolean] =
    Cli.command.parse(List("restore", "daily-1", "--bucket", "b", "--dataset", "tank/data") ++ args, Map.empty) match
      case Right((_, _, Cmd.Restore(_, _, force, _))) => Some(force)
      case _                                         => None

  test("restore accepts -F as a synonym of --force") {
    assertEquals(restoreForce(), Some(false))
    assertEquals(restoreForce("--force"), Some(true))
    assertEquals(restoreForce("-F"), Some(true))
  }
