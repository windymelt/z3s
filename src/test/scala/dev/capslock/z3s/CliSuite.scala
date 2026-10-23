package dev.capslock.z3s

class CliSuite extends UnitSpec:
  private def restoreForce(args: String*): Option[Boolean] =
    Cli.command.parse(List("restore", "daily-1", "--bucket", "b", "--dataset", "tank/data") ++ args, Map.empty) match
      case Right((_, _, Cmd.Restore(_, _, force, _))) => Some(force)
      case _                                         => None

  test("restore accepts -F as a synonym of --force") {
    restoreForce() should === (Some(false))
    restoreForce("--force") should === (Some(true))
    restoreForce("-F") should === (Some(true))
  }
