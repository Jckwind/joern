package io.joern.joerncli

import better.files.Dsl.*
import better.files.File
import flatgraph.{Accessors, Edge, GNode}
import flatgraph.formats.ExportResult
import flatgraph.formats.dot.DotExporter
import flatgraph.formats.graphml.GraphMLExporter
import flatgraph.formats.graphson.GraphSONExporter
import flatgraph.formats.neo4jcsv.{ColumnDefinitions, ColumnType, HeaderFileSuffix, DataFileSuffix}
import io.joern.dataflowengineoss.DefaultSemantics
import io.joern.dataflowengineoss.layers.dataflows.*
import io.joern.dataflowengineoss.semanticsloader.Semantics
import io.joern.joerncli.CpgBasedTool.exitIfInvalid
import io.joern.x2cpg.layers.*
import io.shiftleft.codepropertygraph.generated.Cpg
import io.shiftleft.codepropertygraph.generated.NodeTypes
import io.shiftleft.semanticcpg.language.*
import io.shiftleft.semanticcpg.layers.*

import com.github.tototoshi.csv.*
import flatgraph.formats.{ExportResult, Exporter, writeFile}
import flatgraph.{Edge, GNode, Graph, Schema}
import java.nio.file.{Path, Paths, Files}
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using
import scala.util.{Try, Success, Failure}
import scala.jdk.OptionConverters.RichOptional

object JoernExport {

  case class Config(
    cpgFileName: String = "cpg.bin",
    outDir: String = "out",
    repr: Representation.Value = Representation.Cpg14,
    format: Format.Value = Format.Dot
  )

  /** Choose from either a subset of the graph, or the entire graph (all).
    */
  object Representation extends Enumeration {
    val Ast, Cfg, Ddg, Cdg, Pdg, Cpg14, Cpg, All = Value

    lazy val byNameLowercase: Map[String, Value] =
      values.map { value =>
        value.toString.toLowerCase -> value
      }.toMap

    def withNameIgnoreCase(s: String): Value =
      byNameLowercase.getOrElse(s, throw new NoSuchElementException(s"No value found for '$s'"))

  }
  object Format extends Enumeration {
    val Dot, Neo4jCsv, Graphml, Graphson = Value

    lazy val byNameLowercase: Map[String, Value] =
      values.map { value =>
        value.toString.toLowerCase -> value
      }.toMap

    def withNameIgnoreCase(s: String): Value =
      byNameLowercase.getOrElse(s, throw new NoSuchElementException(s"No value found for '$s'"))
  }

  def main(args: Array[String]): Unit = {
    parseConfig(args).foreach { config =>
      val outDir = config.outDir
      exitIfInvalid(outDir, config.cpgFileName)
      mkdir(File(outDir))

      Using.resource(CpgBasedTool.loadFromFile(config.cpgFileName)) { cpg =>
        exportCpg(cpg, config.repr, config.format, Paths.get(outDir).toAbsolutePath)
      }
    }
  }

  private def parseConfig(args: Array[String]): Option[Config] = {
    new scopt.OptionParser[Config]("joern-export") {
      head("Dump intermediate graph representations (or entire graph) of code in a given export format")
      help("help")
      arg[String]("cpg")
        .text("input CPG file name - defaults to `cpg.bin`")
        .optional()
        .action((x, c) => c.copy(cpgFileName = x))
      opt[String]('o', "out")
        .text("output directory - will be created and must not yet exist")
        .action((x, c) => c.copy(outDir = x))
      opt[String]("repr")
        .text(
          s"representation to extract: [${Representation.values.toSeq.map(_.toString.toLowerCase).sorted.mkString("|")}] - defaults to `${Representation.Cpg14}`"
        )
        .action((x, c) => c.copy(repr = Representation.withNameIgnoreCase(x)))
      opt[String]("format")
        .action((x, c) => c.copy(format = Format.withNameIgnoreCase(x)))
        .text(
          s"export format, one of [${Format.values.toSeq.map(_.toString.toLowerCase).sorted.mkString("|")}] - defaults to `${Format.Dot}`"
        )
    }.parse(args, Config())
  }

  def exportCpg(cpg: Cpg, representation: Representation.Value, format: Format.Value, outDir: Path): Unit = {
    implicit val semantics: Semantics = DefaultSemantics()
    if (semantics.elements.isEmpty) {
      System.err.println("Warning: semantics are empty.")
    }

    try {
      CpgBasedTool.addDataFlowOverlayIfNonExistent(cpg)
    } catch {
      case e: Exception =>
        System.err.println(s"Warning: Failed to add dataflow overlay. Proceeding without it. Error: ${e.getMessage}")
    }

    val context = new LayerCreatorContext(cpg)

    format match {
      case Format.Dot if representation == Representation.All || representation == Representation.Cpg =>
        exportWithFlatgraphFormat(cpg, representation, outDir, DotExporter)
      case Format.Dot =>
        exportDot(representation, outDir, context)
      case Format.Neo4jCsv =>
        exportNeo4jCsv(cpg, representation, outDir)
      case Format.Graphml =>
        exportWithFlatgraphFormat(cpg, representation, outDir, GraphMLExporter)
      case Format.Graphson =>
        exportWithFlatgraphFormat(cpg, representation, outDir, GraphSONExporter)
      case other =>
        throw new NotImplementedError(s"repr=$representation not yet supported for format=$format")
    }
  }

  private def exportDot(repr: Representation.Value, outDir: Path, context: LayerCreatorContext): Unit = {
    val outDirStr = outDir.toString
    import Representation._
    repr match {
      case Ast   => new DumpAst(AstDumpOptions(outDirStr)).create(context)
      case Cfg   => new DumpCfg(CfgDumpOptions(outDirStr)).create(context)
      case Ddg   => new DumpDdg(DdgDumpOptions(outDirStr)).create(context)
      case Cdg   => new DumpCdg(CdgDumpOptions(outDirStr)).create(context)
      case Pdg   => new DumpPdg(PdgDumpOptions(outDirStr)).create(context)
      case Cpg14 => new DumpCpg14(Cpg14DumpOptions(outDirStr)).create(context)
      case other => throw new NotImplementedError(s"repr=$repr not yet supported for this format")
    }
  }

  private def exportWithFlatgraphFormat(
    cpg: Cpg,
    repr: Representation.Value,
    outDir: Path,
    exporter: flatgraph.formats.Exporter
  ): Unit = {
    try {
      val ExportResult(nodeCount, edgeCount, _, additionalInfo) = repr match {
        case Representation.All =>
          exporter.runExport(cpg.graph, outDir)
        case Representation.Cpg =>
          if (cpg.graph.nodeCount(NodeTypes.METHOD) > 0) {
            val windowsFilenameDeduplicationHelper = mutable.Set.empty[String]
            splitByMethod(cpg).iterator
              .map { methodSubGraph =>
                try {
                  val relativeFilename = sanitizedFileName(
                    methodSubGraph.methodName,
                    methodSubGraph.methodFilename,
                    "", // Pass an empty string for directories
                    windowsFilenameDeduplicationHelper
                  )
                  val outFileName = outDir.resolve(relativeFilename)
                  exporter.runExport(cpg.graph.schema, methodSubGraph.nodes, methodSubGraph.edges, outFileName)
                } catch {
                  case e: Exception =>
                    println(s"Error exporting method ${methodSubGraph.methodName} from file ${methodSubGraph.methodFilename}: ${e.getClass.getSimpleName} - ${e.getMessage}")
                    println(s"Node types: ${methodSubGraph.nodes.map(_.getClass.getSimpleName).mkString(", ")}")
                    println(s"Edge types: ${methodSubGraph.edges.map(_.getClass.getSimpleName).mkString(", ")}")
                    e.printStackTrace()
                    emptyExportResult
                }
              }
              .reduce(plus)
          } else {
            emptyExportResult
          }
        case Representation.Cpg14 =>
          exporter.runExport(cpg.graph, outDir)
        case other =>
          throw new NotImplementedError(s"repr=$repr not yet supported for this format")
      }

      println(s"exported $nodeCount nodes, $edgeCount edges into $outDir")
      additionalInfo.foreach(println)
    } catch {
      case e: Exception =>
        println(s"Error during export: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  /** for each method in the cpg: recursively traverse all AST edges to get the subgraph of nodes within this method add
    * the method and this subgraph to the export add all edges between all of these nodes to the export
    */
  private def splitByMethod(cpg: Cpg): IterableOnce[MethodSubGraph] = {
    cpg.method.map { method =>
      MethodSubGraph(
        methodName = Option(method.name).getOrElse("<unknown>"),
        methodFilename = Option(method.filename).getOrElse("<empty>"),
        nodes = Option(method.ast).map(_.toSet).getOrElse(Set.empty)
      )
    }
  }

  /** @param windowsFilenameDeduplicationHelper
    *   utility map to ensure we don't override output files for identical method names
    */
  private def sanitizedFileName(
    methodName: String,
    methodFilename: String,
    fileExtension: String,
    windowsFilenameDeduplicationHelper: mutable.Set[String]
  ): String = {
    val sanitizedMethodName = methodName.replaceAll("[^a-zA-Z0-9-_\\.]", "_")
    val sanitizedFilename =
      if (scala.util.Properties.isWin) {
        // Windows-specific logic (unchanged)
        if (windowsFilenameDeduplicationHelper.contains(sanitizedMethodName)) {
          sanitizedFileName(s"${sanitizedMethodName}_", methodFilename, fileExtension, windowsFilenameDeduplicationHelper)
        } else {
          windowsFilenameDeduplicationHelper.add(sanitizedMethodName)
          sanitizedMethodName
        }
      } else {
        // Non-Windows logic (updated to handle directories correctly)
        val sanitizedPath = methodFilename.split("[/\\\\]").map(_.replaceAll("[^a-zA-Z0-9-_\\.]", "_")).mkString("_")
        s"${sanitizedPath}_${sanitizedMethodName}"
      }

    // Only add the file extension if it's a file, not a directory
    if (fileExtension.nonEmpty) s"$sanitizedFilename.$fileExtension" else sanitizedFilename
  }

  private def plus(resultA: ExportResult, resultB: ExportResult): ExportResult = {
    ExportResult(
      nodeCount = resultA.nodeCount + resultB.nodeCount,
      edgeCount = resultA.edgeCount + resultB.edgeCount,
      files = resultA.files ++ resultB.files,
      additionalInfo = resultA.additionalInfo
    )
  }

  private def emptyExportResult = ExportResult(0, 0, Seq.empty, Option("Empty CPG"))

  case class MethodSubGraph(methodName: String, methodFilename: String, nodes: Set[GNode]) {
    def edges: Set[Edge] = {
      (for {
        node <- nodes
        edge <- Option(Accessors.getEdgesOut(node)).getOrElse(Seq.empty)
        if nodes.contains(edge.dst)
      } yield edge).toSet
    }
  }

  private def exportNeo4jCsv(cpg: Cpg, repr: Representation.Value, outDir: Path): Unit = {
    try {
      val exportResult = repr match {
        case Representation.All | Representation.Cpg | Representation.Ast | Representation.Cfg | Representation.Ddg | Representation.Cdg | Representation.Pdg | Representation.Cpg14 =>
          Neo4jCsvExporter.runExport(cpg.graph.schema, cpg.graph.nodes().toSeq, cpg.graph.allEdges.toSeq, outDir)

        case other =>
          throw new NotImplementedError(s"repr=$repr not yet supported for this format")
      }

      logExportResult(exportResult, outDir)
    } catch {
      case e: Exception =>
        logError("Error during export", e)
    }
  }

  private def logExportResult(result: ExportResult, outDir: Path): Unit = {
    println(s"exported ${result.nodeCount} nodes, ${result.edgeCount} edges into $outDir")
    result.additionalInfo.foreach(println)
  }

  private def logError(message: String, e: Exception): Unit = {
    println(s"$message: ${e.getMessage}")
    e.printStackTrace()
  }
}

object Neo4jCsvExporter extends Exporter {

  override def defaultFileExtension: String = "csv"

  /**
    *
    * For both nodes and relationships, we first write the data file and to derive the property types from their runtime types. We will
    * write columns for all declared properties, because we only know which ones are actually in use *after* traversing all elements.
    */
  override def runExport(schema: Schema, nodes: IterableOnce[GNode], edges: IterableOnce[Edge], outDir: Path): ExportResult = {
    try {
      val nodesByLabel = nodes.iterator.toSeq.groupBy(_.label).filter(_._2.nonEmpty)
      val CountAndFiles(nodeCount, nodeFiles) = nodesByLabel
        .map { case (label, nodes) =>
          exportNodes(nodes, label, schema, outDir)
        }
        .reduce(_.plus(_))

      val CountAndFiles(edgeCount, edgeFiles) = exportEdges(edges, outDir)

      ExportResult(
        nodeCount = nodeCount,
        edgeCount = edgeCount,
        files = nodeFiles ++ edgeFiles,
        additionalInfo = Option("Exported nodes and edges to CSV files.")
      )
    } catch {
      case e: Exception =>
        println(s"Error during export: ${e.getMessage}")
        e.printStackTrace()
        ExportResult(0, 0, Seq.empty, Option(s"Error during export: ${e.getMessage}"))
    }
  }

  private def exportNodes(nodes: IterableOnce[GNode], label: String, schema: Schema, outDir: Path): CountAndFiles = {
    val dataFile = outDir.resolve(s"nodes_${label}${DataFileSuffix}.csv")
    val headerFile = outDir.resolve(s"nodes_${label}${HeaderFileSuffix}.csv")

    Files.createDirectories(dataFile.getParent)

    val propertyNames = schema.getNodePropertyNames(label)
    val columnDefinitions = new ColumnDefinitions(propertyNames.toList)
    var nodeCount = 0

    Using(CSVWriter.open(dataFile.toFile)) { writer =>
      try {
        nodes.iterator.foreach { node =>
          try {
            val specialColumns = Seq(node.id.toString, node.label)
            var properties = Accessors.getNodeProperties(node).toMap
            val propertyValueColumns = columnDefinitions.propertyValues(node.propertyOption)
            writer.writeRow(specialColumns ++ propertyValueColumns)
            nodeCount += 1
          } catch {
            case e: Exception =>
              println(s"Error processing node with ID: ${node.id}, Label: ${node.label}. Error: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      } catch {
        case e: Exception =>
          println(s"Error processing nodes: ${e.getMessage}")
          e.printStackTrace()
      }
    }.recover {
      case e: Exception =>
        println(s"Error writing nodes to CSV: ${e.getMessage}")
        e.printStackTrace()
    }

    if (nodeCount == 0) {
      println(s"No nodes found for label $label")
      return CountAndFiles(0, Seq.empty)
    }

    writeSingleLineCsv(headerFile, Seq(ColumnType.Id, ColumnType.Label) ++ columnDefinitions.propertiesWithTypes)
    CountAndFiles(nodeCount, Seq(headerFile, dataFile))
  }

  /** write edges of all labels */
  private def exportEdges(edges: IterableOnce[Edge], outputRootDirectory: Path): CountAndFiles = {
    val edgeFilesContextByLabel = mutable.Map.empty[String, EdgeFilesContext]
    var count                   = 0

    edges.iterator.foreach { edge =>
      val label = edge.label
      val context = edgeFilesContextByLabel.getOrElseUpdate(
        label, {
          // first time we encounter an edge of this type - create the columnMapping and write the header file
          val headerFile        = outputRootDirectory.resolve(s"edges_$label$HeaderFileSuffix.csv")
          // header file to be written at the very end, with complete ColumnDefByName
          val dataFile          = outputRootDirectory.resolve(s"edges_$label$DataFileSuffix.csv")
          val dataFileWriter    = CSVWriter.open(dataFile.toFile, append = false)
          val columnDefinitions = new ColumnDefinitions(edge.propertyName.toList) // flatgraph only supports edges with 1 property
          println(s"label: $label")
          println(s"edge.propertyName: ${edge.propertyName}")
          println(s"columnDefinitions: ${columnDefinitions.propertiesWithTypes}")
          EdgeFilesContext(label, headerFile, dataFile, dataFileWriter, columnDefinitions)
        }
      )

      val specialColumns = Seq(edge.src.id.toString, edge.dst.id.toString, edge.label)
      val propertyValueColumns = context.columnDefinitions.propertyValues { propertyName =>
        edge.propertyName.flatMap { edgePropertyName =>
          if (propertyName == edgePropertyName)
            edge.propertyMaybe
          else
            None
        }
      }
      context.dataFileWriter.writeRow(specialColumns ++ propertyValueColumns)
      count += 1
    }

    val files = edgeFilesContextByLabel.values.flatMap {
      case EdgeFilesContext(label, headerFile, dataFile, dataFileWriter, columnDefinitions) =>
        println(s"columnDefinitions: ${columnDefinitions.propertiesWithTypes}")
        writeSingleLineCsv(headerFile, Seq(ColumnType.StartId, ColumnType.EndId, ColumnType.Type) ++ columnDefinitions.propertiesWithTypes)

        dataFileWriter.flush()
        dataFileWriter.close()

        Seq(headerFile, dataFile)
    }.toSeq

    CountAndFiles(count, files)
  }

  private def sanitizeLabel(label: String): String = {
    label.replaceAll("[^a-zA-Z0-9]", "_")
  }

  private def writeSingleLineCsv(outputFile: Path, entries: Seq[Any]): Unit = {
    Using.resource(CSVWriter.open(outputFile.toFile, append = false)) { writer =>
      writer.writeRow(entries)
    }
  }

  private case class EdgeFilesContext(
    label: String,
    headerFile: Path,
    dataFile: Path,
    dataFileWriter: CSVWriter,
    columnDefinitions: ColumnDefinitions
  )

  case class CountAndFiles(count: Int, files: Seq[Path]) {
    def plus(other: CountAndFiles): CountAndFiles =
      CountAndFiles(count + other.count, files ++ other.files)
  }

}
