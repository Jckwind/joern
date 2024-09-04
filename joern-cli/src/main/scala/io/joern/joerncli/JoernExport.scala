package io.joern.joerncli

import better.files.Dsl.*
import better.files.File
import flatgraph.{Accessors, Edge, GNode}
import flatgraph.formats.ExportResult
import flatgraph.formats.dot.DotExporter
import flatgraph.formats.graphml.GraphMLExporter
import flatgraph.formats.graphson.GraphSONExporter
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
import flatgraph.formats.iterableForList

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsScala

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
          Neo4jCsvExporter.runExport(cpg.graph.schema, cpg.graph.allNodes.toSeq, cpg.graph.allEdges.toSeq, outDir)

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

  private[Neo4jCsvExporter] object FileType extends Enumeration {
    val Nodes, Relationships = Value
  }

  val HeaderFileSuffix = "_header"
  val DataFileSuffix   = "_data"
  val CypherFileSuffix = "_cypher"

  object ColumnType extends Enumeration {
    // defining 'stable' string so we can pattern match on them
    val LabelMarker = ":LABEL"
    val TypeMarker  = ":TYPE"
    val ArrayMarker = "[]"

    // special types for nodes
    val Id    = Value(":ID")
    val Label = Value(LabelMarker)

    // special types for relationships
    val Type    = Value(TypeMarker)
    val StartId = Value(":START_ID")
    val EndId   = Value(":END_ID")

    // regular data types
    val Int           = Value("int")
    val Long          = Value("long")
    val Float         = Value("float")
    val Double        = Value("double")
    val Boolean       = Value("boolean")
    val Byte          = Value("byte")
    val Short         = Value("short")
    val Char          = Value("char")
    val String        = Value("string")
    val Point         = Value("point")
    val Date          = Value("date")
    val LocalTime     = Value("localtime")
    val Time          = Value("time")
    val LocalDateTime = Value("localdatetime")
    val DateTime      = Value("datetime")
    val Duration      = Value("duration")
  }

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
            import scala.jdk.CollectionConverters._
            for ((key, value) <- node.propertiesMap.asScala) {
              columnDefinitions.updateWith(key, value)
            }
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
  
  private def exportEdges(edges: IterableOnce[Edge], outputRootDirectory: Path): CountAndFiles = {
    val edgeFilesContextByLabel = mutable.Map.empty[String, EdgeFilesContext]
    var count = 0

    // Map to store properties of each edge, keyed by (srcId, dstId, label)
    // PERFORMANCE NOTE: This map could potentially consume a large amount of memory for graphs with many edges
    val edgeProperties = mutable.Map.empty[(Long, Long, String), mutable.Map[String, Any]]

    // Iterate over all edges to collect their properties
    // PERFORMANCE NOTE: This iteration could be slow for large graphs, as it processes all edges in memory
    edges.iterator.foreach { edge =>
      val key = (edge.src.id, edge.dst.id, edge.label)
      val properties = edgeProperties.getOrElseUpdate(key, mutable.Map.empty[String, Any])
      edge.propertyName.foreach { name =>
        edge.propertyMaybe.foreach { value =>
          properties(name) = value
        }
      }
    }

    // Iterate over the collected edge properties to write them to the CSV
    // PERFORMANCE NOTE: This second iteration over all edges could be slow for large graphs
    edgeProperties.foreach { case ((srcId, dstId, label), properties) =>
      val context = edgeFilesContextByLabel.getOrElseUpdate(
        label, {
          // First time we encounter an edge of this type - create the columnMapping and write the header file
          val headerFile = outputRootDirectory.resolve(s"edges_${sanitizeLabel(label)}$HeaderFileSuffix.csv")
          val dataFile = outputRootDirectory.resolve(s"edges_${sanitizeLabel(label)}$DataFileSuffix.csv")
          // PERFORMANCE NOTE: Opening a new file writer for each edge type could lead to many open file handles
          val dataFileWriter = CSVWriter.open(dataFile.toFile, append = false)
          val columnDefinitions = new ColumnDefinitions(properties.keys.toList)
          EdgeFilesContext(label, headerFile, dataFile, dataFileWriter, columnDefinitions)
        }
      )

      val specialColumns = Seq(srcId.toString, dstId.toString, label)
      val propertyValueColumns = context.columnDefinitions.propertyValues { propertyName =>
        properties.get(propertyName)
      }
      context.dataFileWriter.writeRow(specialColumns ++ propertyValueColumns)
      count += 1
    }

    val files = edgeFilesContextByLabel.values.flatMap {
      case EdgeFilesContext(label, headerFile, dataFile, dataFileWriter, columnDefinitions) =>
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

  private[Neo4jCsvExporter] class ColumnDefinitions(propertyNames: Iterable[String]) {
    sealed trait ColumnDef
  case class ScalarColumnDef(valueType: ColumnType.Value)                                              extends ColumnDef
  case class ArrayColumnDef(valueType: Option[ColumnType.Value], iteratorAccessor: Any => Iterable[?]) extends ColumnDef
    private val propertyNamesOrdered     = propertyNames.toSeq.sorted
    private val _columnDefByPropertyName = mutable.Map.empty[String, ColumnDef]

    def columnDefByPropertyName(name: String): Option[ColumnDef] = _columnDefByPropertyName.get(name)

    def updateWith(propertyName: String, value: Any): ColumnDef = {
      _columnDefByPropertyName
        .updateWith(propertyName) {
          case None =>
            // we didn't see this property before - try to derive it's type from the runtime class
            Option(deriveNeo4jType(value))
          case Some(ArrayColumnDef(None, _)) =>
            // value is an array that we've seen before, but we don't have the valueType yet, most likely because previous occurrences were empty arrays
            Option(deriveNeo4jType(value))
          case completeDef =>
            completeDef // we already have everything we need, no need to change anything
        }
        .get
    }

    /** for header file */
    def propertiesWithTypes: Seq[String] = {
      propertyNamesOrdered.map { name =>
        columnDefByPropertyName(name) match {
          case Some(ScalarColumnDef(valueType)) =>
            s"$name:$valueType"
          case Some(ArrayColumnDef(Some(valueType), _)) =>
            s"$name:$valueType[]"
          case _ =>
            name
        }
      }
    }

    /** for data file updates our internal `_columnDefByPropertyName` model with type information based on runtime values, so that we later
      * have all metadata required for the header file
      */
    def propertyValues(byNameAccessor: String => Option[?]): Seq[String] = {
      propertyNamesOrdered.map { propertyName =>
        byNameAccessor(propertyName) match {
          case None =>
            "" // property value empty for this element
          case Some(value) =>
            updateWith(propertyName, value) match {
              case ScalarColumnDef(_) =>
                value.toString // scalar property value
              case ArrayColumnDef(_, iteratorAccessor) =>
                /** Array property value - separated by `;` according to the spec
                  *
                  * Note: if all instances of this array property type are empty, we will not have the valueType (because it's derived from
                  * the runtime class). At the same time, it doesn't matter for serialization, because the csv entry is always empty for all
                  * empty arrays.
                  */
                iteratorAccessor(value).mkString(";")
            }
        }
      }
    }

    /** for cypher file <rant> why does neo4j have 4 different ways to import a CSV, out of which only one works, and really the only help we
      * get is a csv file reader, and we need to specify exactly how each column needs to be parsed and mapped...? </rant>
      */
    def propertiesMappingsForCypher(startIndex: Int): Seq[String] = {
      var idx = startIndex - 1
      propertyNamesOrdered.map { name =>
        idx += 1
        val accessor = s"line[$idx]"
        columnDefByPropertyName(name) match {
          case Some(ScalarColumnDef(columnType)) =>
            val adaptedAccessor =
              cypherScalarConversionFunctionMaybe(columnType)
                .map(f => s"$f($accessor)")
                .getOrElse(accessor)
            s"$name: $adaptedAccessor"
          case Some(ArrayColumnDef(columnType, _)) =>
            val accessor = s"""split(line[$idx], ";")"""
            val adaptedAccessor =
              columnType
                .flatMap(cypherListConversionFunctionMaybe)
                .map(f => s"$f($accessor)")
                .getOrElse(accessor)
            s"$name: $adaptedAccessor"
          case None =>
            s"$name: $accessor"
        }
      }
    }

    /** optionally choose one of https://neo4j.com/docs/cypher-manual/current/functions/scalar/, depending on the columnType
      */
    private def cypherScalarConversionFunctionMaybe(columnType: ColumnType.Value): Option[String] = {
      columnType match {
        case ColumnType.Id | ColumnType.Int | ColumnType.Long | ColumnType.Byte | ColumnType.Short =>
          Some("toInteger")
        case ColumnType.Float | ColumnType.Double =>
          Some("toFloat")
        case ColumnType.Boolean =>
          Some("toBoolean")
        case _ => None
      }
    }

    /** optionally choose one of https://neo4j.com/docs/cypher-manual/current/functions/list/#functions-tobooleanlist, depending on the
      * columnType
      */
    private def cypherListConversionFunctionMaybe(columnType: ColumnType.Value): Option[String] = {
      columnType match {
        case ColumnType.Id | ColumnType.Int | ColumnType.Long | ColumnType.Byte | ColumnType.Short =>
          Some("toIntegerList")
        case ColumnType.Float | ColumnType.Double =>
          Some("toFloatList")
        case ColumnType.Boolean =>
          Some("toBooleanList")
        case ColumnType.String =>
          Some("toStringList")
        case _ => None
      }
    }

    /** derive property types based on the runtime class note: this ignores the edge case that there may be different runtime types for the
      * same property
      */
    private def deriveNeo4jType(value: Any): ColumnDef = {
      def deriveNeo4jTypeForArray(iteratorAccessor: Any => Iterable[?]): ArrayColumnDef = {
        // Iterable is immutable, so we can safely (try to) get it's first element
        val valueTypeMaybe = iteratorAccessor(value).iterator
          .nextOption()
          .map(_.getClass)
          .map(deriveNeo4jTypeForScalarValue)
        ArrayColumnDef(valueTypeMaybe, iteratorAccessor)
      }

      value match {
        case _: Iterable[_] =>
          deriveNeo4jTypeForArray(_.asInstanceOf[Iterable[?]])
        case _: IterableOnce[_] =>
          deriveNeo4jTypeForArray(_.asInstanceOf[IterableOnce[?]].iterator.toSeq)
        case _: java.lang.Iterable[_] =>
          deriveNeo4jTypeForArray(_.asInstanceOf[java.lang.Iterable[?]].asScala)
        case _: Array[_] =>
          deriveNeo4jTypeForArray(x => ArraySeq.unsafeWrapArray(x.asInstanceOf[Array[?]]))
        case scalarValue =>
          ScalarColumnDef(deriveNeo4jTypeForScalarValue(scalarValue.getClass))
      }
    }

    private def deriveNeo4jTypeForScalarValue(clazz: Class[?]): ColumnType.Value = {
      if (clazz == classOf[String])
        ColumnType.String
      else if (clazz == classOf[Int] || clazz == classOf[Integer] || clazz == classOf[Long] || clazz == classOf[java.lang.Long])
        // neo4j prefers Long over Int
        ColumnType.Long
      else if (clazz == classOf[Float] || clazz == classOf[java.lang.Float])
        ColumnType.Float
      else if (clazz == classOf[Double] || clazz == classOf[java.lang.Double])
        ColumnType.Double
      else if (clazz == classOf[Boolean] || clazz == classOf[java.lang.Boolean])
        ColumnType.Boolean
      else if (clazz == classOf[Byte] || clazz == classOf[java.lang.Byte])
        ColumnType.Byte
      else if (clazz == classOf[Short] || clazz == classOf[java.lang.Short])
        ColumnType.Short
      else if (clazz == classOf[Char])
        ColumnType.Char
      else
        throw new NotImplementedError(s"unable to derive a Neo4j type for given runtime type $clazz")
    }
  }


}
