package io.joern.x2cpg.frontendspecific.pysrc2cpg

import io.joern.x2cpg.passes.frontend.XInheritanceFullNamePass
import io.shiftleft.codepropertygraph.generated.Cpg

/** Using some basic heuristics, will try to resolve type full names from types found within the CPG. Requires
  * ImportPass as a pre-requisite.
  */
class PythonInheritanceNamePass(cpg: Cpg) extends XInheritanceFullNamePass(cpg) {

  override val moduleName: String = "<module>"
  override val fileExt: String    = ".py"

  override protected def xTypeFullName(importedType: String, importedPath: String): (String, String) = {
    val combinedPath = s"$importedPath$pathSep$importedType"
    combinedPath.split(pathSep).lastOption match {
      case Some(typeName) =>
        val modulePath = combinedPath.stripSuffix(s"$pathSep$typeName")
        val fullName = s"$modulePath$pathSep$moduleName$pathSep$typeName"
        (typeName, fullName)
      case None => (combinedPath, combinedPath)
    }
  }
}
