package io.joern.console.cpgcreation

import io.joern.console.FrontendConfig
import io.joern.x2cpg.X2Cpg
import io.joern.x2cpg.frontendspecific.pysrc2cpg
import io.joern.x2cpg.frontendspecific.pysrc2cpg.*
import io.joern.x2cpg.passes.base.AstLinkerPass
import io.joern.x2cpg.passes.callgraph.NaiveCallLinker
import io.joern.x2cpg.passes.frontend.{XTypeRecoveryConfig, TypeRecoveryParserConfig}
import io.shiftleft.codepropertygraph.generated.Cpg
import io.joern.x2cpg.{X2Cpg, X2CpgConfig}
import scopt.OParser
import java.nio.file.Paths
import java.nio.file.Path
import scala.util.Try

case class PythonSrcCpgGenerator(config: FrontendConfig, rootPath: Path) extends CpgGenerator {
  private lazy val command: Path = if (isWin) rootPath.resolve("pysrc2cpg.bat") else rootPath.resolve("pysrc2cpg")
  private var typeRecoveryConfig: XTypeRecoveryConfig = null

  /** Generate a CPG for the given input path. Returns the output path, or None, if no CPG was generated.
    */
  val first_two_params = config.cmdLineParams.toSeq.take(2)
  val params = config.cmdLineParams.toSeq.drop(2)
  override def generate(inputPath: String, outputPath: String = "cpg.bin.zip"): Try[String] = {
    typeRecoveryConfig = XTypeRecoveryConfig.parse(first_two_params)
    val arguments = Seq(inputPath, "-o", outputPath) ++ params
    runShellCommand(command.toString, arguments).map(_ => outputPath)
  }

  override def isAvailable: Boolean =
    command.toFile.exists

  override def applyPostProcessingPasses(cpg: Cpg): Cpg = {
    if (typeRecoveryConfig != null) {
      typeRecoveryConfig = XTypeRecoveryConfig.parse(first_two_params)
      pysrc2cpg.postProcessingPasses(cpg, typeRecoveryConfig).foreach(_.createAndApply())
    }

    cpg
  }

  override def isJvmBased = true
}
