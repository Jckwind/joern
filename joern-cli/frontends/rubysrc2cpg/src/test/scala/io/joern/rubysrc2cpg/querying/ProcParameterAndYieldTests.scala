package io.joern.rubysrc2cpg.querying

import io.joern.rubysrc2cpg.passes.Defines.RubyOperators
import io.joern.rubysrc2cpg.testfixtures.RubyCode2CpgFixture
import io.shiftleft.codepropertygraph.generated.nodes.*
import io.shiftleft.semanticcpg.language.*
import org.scalatest.Inspectors

class ProcParameterAndYieldTests extends RubyCode2CpgFixture with Inspectors {

  "a method with an explicit proc parameter should create an invocation of it's `call` member" in {
    val cpg = code("def foo(&b) yield end")

    val foo = cpg.method("foo").head

    val bParam = foo.parameter.last
    bParam.name shouldBe "b"
    bParam.code shouldBe "&b"
    bParam.index shouldBe 1

    inside(foo.call.nameExact("call").argument.l) { case selfBase :: Nil =>
      selfBase.code shouldBe "b"
    }
  }

  "a singleton method with an explicit proc parameter should create an invocation of it's `call` member" in {
    val cpg = code("def self.foo(&b) yield end")

    val foo = cpg.method("foo").head

    val bParam = foo.parameter.last
    bParam.name shouldBe "b"
    bParam.code shouldBe "&b"
    bParam.index shouldBe 1

    inside(foo.call.nameExact("call").argument.l) { case selfBase :: Nil =>
      selfBase.code shouldBe "b"
    }
  }

  "a method with an implicit proc parameter should create an invocation using a unique parameter name" in {
    val cpg = code("""
        |def foo() yield end
        |def self.bar() yield end
        |""".stripMargin)

    val foo = cpg.method("foo").head
    val bar = cpg.method("bar").head

    val fooParam = foo.parameter.last
    fooParam.name shouldBe "<proc-param-0>"
    fooParam.code shouldBe "&<proc-param-0>"
    fooParam.index shouldBe 1

    val barParam = bar.parameter.last
    barParam.name shouldBe "<proc-param-1>"
    barParam.code shouldBe "&<proc-param-1>"
    barParam.index shouldBe 1

    foo.call.nameExact("call").argument.isIdentifier.name.l shouldBe List("<proc-param-0>")
    bar.call.nameExact("call").argument.isIdentifier.name.l shouldBe List("<proc-param-1>")
  }

  "a method with an implicit proc parameter should create an invocation of it's `call` member with given arguments" in {
    val cpg = code("def foo(x) yield(x) end")

    val foo = cpg.method("foo").head

    val List(xParam, procParam) = foo.parameter.l.takeRight(2)

    xParam.name shouldBe "x"
    xParam.index shouldBe 1

    procParam.name shouldBe "<proc-param-0>"
    procParam.code shouldBe "&<proc-param-0>"
    procParam.index shouldBe 2

    inside(foo.call.nameExact("call").argument.l) { case selfBase :: x :: Nil =>
      selfBase.code shouldBe "<proc-param-0>"
      selfBase.argumentIndex shouldBe 0
      x.code shouldBe "x"
      x.argumentIndex shouldBe 1
    }
  }

  "a method without a yield nor proc parameter should not have either modelled" in {
    val cpg1 = code("def foo() end")
    val cpg2 = code("def self.foo() end")
    val cpgs = List(cpg1, cpg2)

    forAll(cpgs)(cpg => {
      cpg.method("foo").parameter.code("&.*").name.l should be(empty)
      cpg.method("foo").call.nameExact("call").name.l should be(empty)
    })
  }

  "A Yield statement with multiple arguments" in {
    val cpg = code("""
        |def foo
        | yield 1, :z => 2
        |end
        |""".stripMargin)

    inside(cpg.method.name("foo").call.nameExact("call").l) {
      case yieldCall :: Nil =>
        inside(yieldCall.argument.l) {
          case (base: Identifier) :: (literalArg: Literal) :: (assocArg: Call) :: Nil =>
            base.name shouldBe "<proc-param-0>"
            base.code shouldBe "<proc-param-0>"

            literalArg.code shouldBe "1"
            assocArg.code shouldBe (":z => 2")

            assocArg.methodFullName shouldBe RubyOperators.association
            inside(assocArg.argument.l) {
              case (key: Literal) :: (value: Literal) :: Nil =>
                key.code shouldBe ":z"
                value.code shouldBe "2"
              case xs => fail(s"Expected 2 arguments for assoc call, got ${xs.code.mkString(",")}")
            }
          case xs => fail(s"Expected two arguments for yieldCall, got ${xs.code.mkString(",")}")
        }
      case xs => fail(s"Expected one call for yield, got ${xs.code.mkString(",")}")
    }
  }
}
