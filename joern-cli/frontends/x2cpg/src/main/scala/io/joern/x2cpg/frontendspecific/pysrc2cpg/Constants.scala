package io.joern.x2cpg.frontendspecific.pysrc2cpg

import io.shiftleft.codepropertygraph.generated.nodes.AstNode

object Constants {
  val builtinPrefix = "__builtin."

  val ANY                = "ANY"
  val GLOBAL_NAMESPACE   = "<global>"
  val builtinStrType     = s"${builtinPrefix}str"
  val builtinBytesType   = s"${builtinPrefix}bytes"
  val builtinIntType     = s"${builtinPrefix}int"
  val builtinFloatType   = s"${builtinPrefix}float"
  val builtinComplexType = s"${builtinPrefix}complex"

  val moduleName = "<module>"
  val initName   = "__init__"

  // Add the getTypes function
  def getTypes(node: AstNode): Set[String] = {
    val typeFullNames = node.properties.get("TYPE_FULL_NAME").map(_.toString.split(",").toSet).getOrElse(Set.empty)
    val dynamicTypeHints = node.properties.get("DYNAMIC_TYPE_HINT_FULL_NAME").map(_.toString.split(",").toSet).getOrElse(Set.empty)
    (typeFullNames ++ dynamicTypeHints).filterNot(_ == ANY)
  }
}
