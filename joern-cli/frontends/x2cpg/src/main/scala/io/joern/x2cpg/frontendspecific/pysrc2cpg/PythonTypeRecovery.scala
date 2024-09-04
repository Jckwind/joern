package io.joern.x2cpg.frontendspecific.pysrc2cpg

import io.joern.x2cpg.passes.frontend.{RecoverForXCompilationUnit, XTypeRecovery, XTypeRecoveryState}
import io.shiftleft.codepropertygraph.generated.Cpg
import io.shiftleft.codepropertygraph.generated.nodes.File
import io.shiftleft.semanticcpg.language.*
import io.joern.x2cpg.frontendspecific.pysrc2cpg.Constants
import io.joern.x2cpg.passes.frontend.*
import io.shiftleft.codepropertygraph.generated.Cpg
import io.shiftleft.codepropertygraph.generated.nodes.*
import io.shiftleft.codepropertygraph.generated.{Operators, PropertyNames}
import io.shiftleft.semanticcpg.language.*
import io.shiftleft.semanticcpg.language.importresolver.*
import io.shiftleft.semanticcpg.language.operatorextension.OpNodes
import io.shiftleft.semanticcpg.language.operatorextension.OpNodes.FieldAccess
import io.shiftleft.codepropertygraph.generated.DiffGraphBuilder
import io.joern.x2cpg.frontendspecific.pysrc2cpg.Constants.getTypes

private class PythonTypeRecovery(cpg: Cpg, state: XTypeRecoveryState, iteration: Int)
    extends XTypeRecovery[File](cpg, state, iteration) {

  override def compilationUnits: Iterator[File] = cpg.file.iterator

  override def generateRecoveryForCompilationUnitTask(
    unit: File,
    builder: DiffGraphBuilder
  ): RecoverForXCompilationUnit[File] = {
    new RecoverForPythonFile(cpg, unit, builder, state)
  }

}

/** Performs type recovery from the root of a compilation unit level
  */
private class RecoverForPythonFile(cpg: Cpg, cu: File, builder: DiffGraphBuilder, state: XTypeRecoveryState)
    extends RecoverForXCompilationUnit[File](cpg, cu, builder, state) {

  /** Replaces the `this` prefix with the Pythonic `self` prefix for instance methods of functions local to this
    * compilation unit.
    */
  override protected def fromNodeToLocalKey(node: AstNode): Option[LocalKey] =
    node match {
      case n: Method => Option(CallAlias(n.name, Option("self")))
      case _         => SBKey.fromNodeToLocalKey(node)
    }

  override def visitImport(i: Import): Unit = {
    for {
      importedAs <- i.importedAs
      importedEntity <- i.importedEntity
    } {
      val entityName = importedAs
      i.call.tag.flatMap(EvaluatedImport.tagToEvaluatedImport).foreach {
        case ResolvedMethod(fullName, alias, receiver, _) => symbolTable.put(CallAlias(alias, receiver), fullName)
        case ResolvedTypeDecl(fullName, _)                => symbolTable.put(LocalVar(entityName), fullName)
        case ResolvedMember(basePath, memberName, _) =>
          val memberTypes = cpg.typeDecl
            .fullNameExact(basePath)
            .member
            .nameExact(memberName)
            .flatMap(m => m.typeFullName +: m.dynamicTypeHintFullName)
            .filterNot(_ == Constants.ANY)
            .toSet
          symbolTable.put(LocalVar(entityName), memberTypes)
        case UnknownMethod(fullName, alias, receiver, _) =>
          symbolTable.put(CallAlias(alias, receiver), fullName)
        case UnknownTypeDecl(fullName, _) =>
          symbolTable.put(LocalVar(entityName), fullName)
        case UnknownImport(path, _) =>
          symbolTable.put(CallAlias(entityName), path)
          symbolTable.put(LocalVar(entityName), path)
      }
    }
  }

  override def visitAssignments(a: OpNodes.Assignment): Set[String] = {
    a.argumentOut.l match {
      case List(i: Identifier, c: Call) if c.name.isBlank && c.signature.isBlank =>
        c.argument.isMethodRef.headOption.fold(super.visitAssignments(a))(visitIdentifierAssignedToMethodRef(i, _))
      case _ => super.visitAssignments(a)
    }
  }

  /** Determines if a function call is a constructor by following the heuristic that Python classes are typically
    * camel-case and start with an upper-case character.
    */
  override def isConstructor(c: Call): Boolean = isConstructor(c.name) && c.code.endsWith(")")

  override protected def isConstructor(name: String): Boolean =
    name.nonEmpty && name.charAt(0).isUpper

  /** If the parent method is module then it can be used as a field.
    */
  override def isFieldUncached(i: Identifier): Boolean =
    i.method.name.matches(s"(${Constants.moduleName}|${Constants.initName})") || super.isFieldUncached(i)

  override def visitIdentifierAssignedToOperator(i: Identifier, c: Call, operation: String): Set[String] = {
    operation match {
      case "<operator>.listLiteral"  => associateTypes(i, Set(s"${Constants.builtinPrefix}list"))
      case "<operator>.tupleLiteral" => associateTypes(i, Set(s"${Constants.builtinPrefix}tuple"))
      case "<operator>.dictLiteral"  => associateTypes(i, Set(s"${Constants.builtinPrefix}dict"))
      case "<operator>.setLiteral"   => associateTypes(i, Set(s"${Constants.builtinPrefix}set"))
      case "<operator>.listComp" =>
        val elementType = inferComprehensionElementType(c)
        associateTypes(i, Set(s"${Constants.builtinPrefix}list[${elementType}]"))
      case "<operator>.setComp" =>
        val elementType = inferComprehensionElementType(c)
        associateTypes(i, Set(s"${Constants.builtinPrefix}set[${elementType}]"))
      case "<operator>.dictComp" =>
        val (keyType, valueType) = inferDictComprehensionTypes(c)
        associateTypes(i, Set(s"${Constants.builtinPrefix}dict[${keyType}, ${valueType}]"))
      case Operators.conditional     => associateTypes(i, Set(s"${Constants.builtinPrefix}bool"))
      case Operators.indexAccess =>
        c.argument.argumentIndex(1).isCall.foreach(setCallMethodFullNameFromBase)
        visitIdentifierAssignedToIndexAccess(i, c)
      case _ => super.visitIdentifierAssignedToOperator(i, c, operation)
    }
  }

  private def inferComprehensionElementType(c: Call): String = {
    c.argument.headOption.flatMap(x => getTypes(x).headOption).getOrElse(Constants.ANY)
  }

  private def inferDictComprehensionTypes(c: Call): (String, String) = {
    val keyType = c.argument.headOption.flatMap(x => getTypes(x).headOption).getOrElse(Constants.ANY)
    val valueType = c.argument.toList.lift(1).flatMap(x => getTypes(x).headOption).getOrElse(Constants.ANY)
    (keyType, valueType)
  }

  override def visitIdentifierAssignedToConstructor(i: Identifier, c: Call): Set[String] = {
    val constructorPaths = symbolTable.get(c).map(_.stripSuffix(s"$pathSep${Constants.initName}"))
    associateTypes(i, constructorPaths)
  }

  override def visitIdentifierAssignedToCall(i: Identifier, c: Call): Set[String] = {
    // Ignore legacy import representation
    if (c.name.equals("import")) Set.empty
    // Stop custom annotation representation from hitting superclass
    else if (c.name.isBlank) Set.empty
    else {
      c.name match {
        case "len" => associateTypes(i, Set(s"${Constants.builtinPrefix}int"))
        case "type" => associateTypes(i, Set(s"${Constants.builtinPrefix}type"))
        case "isinstance" => associateTypes(i, Set(s"${Constants.builtinPrefix}bool"))
        case "sorted" | "reversed" =>
          val argType = c.argument.headOption.flatMap(x => getTypes(x).headOption).getOrElse(Constants.ANY)
          associateTypes(i, Set(s"${Constants.builtinPrefix}list[$argType]"))
        // ... handle other common built-in functions ...
        case _ => super.visitIdentifierAssignedToCall(i, c)
      }
    }
  }

  override def visitIdentifierAssignedToFieldLoad(i: Identifier, fa: FieldAccess): Set[String] = {
    val fieldParents = getFieldParents(fa)
    fa.astChildren.l match {
      case List(base: Identifier, fi: FieldIdentifier) if base.name.equals("self") && fieldParents.nonEmpty =>
        val referencedFields = cpg.typeDecl.fullNameExact(fieldParents.toSeq*).member.nameExact(fi.canonicalName)
        val globalTypes =
          referencedFields.flatMap(m => m.typeFullName +: m.dynamicTypeHintFullName).filterNot(_ == Constants.ANY).toSet
        associateTypes(i, globalTypes)
      case _ => super.visitIdentifierAssignedToFieldLoad(i, fa)
    }
  }

  override def getTypesFromCall(c: Call): Set[String] = c.name match {
    case "<operator>.listLiteral"  => Set(s"${Constants.builtinPrefix}list")
    case "<operator>.tupleLiteral" => Set(s"${Constants.builtinPrefix}tuple")
    case "<operator>.dictLiteral"  => Set(s"${Constants.builtinPrefix}dict")
    case "<operator>.setLiteral"   => Set(s"${Constants.builtinPrefix}set")
    case _                         => super.getTypesFromCall(c)
  }

  override def getFieldParents(fa: FieldAccess): Set[String] = {
    if (fa.method.name == Constants.moduleName) {
      Set(fa.method.fullName)
    } else if (fa.method.typeDecl.nonEmpty) {
      val parentTypes       = fa.method.typeDecl.fullName.toSet
      val baseTypeFullNames = cpg.typeDecl.fullNameExact(parentTypes.toSeq*).inheritsFromTypeFullName.toSet
      (parentTypes ++ baseTypeFullNames).filterNot(_.matches("(?i)(any|object)"))
    } else {
      super.getFieldParents(fa)
    }
  }

  private def isPyString(s: String): Boolean =
    (s.startsWith("\"") || s.startsWith("'")) && (s.endsWith("\"") || s.endsWith("'"))

  override def getLiteralType(l: Literal): Set[String] = {
    val literalType = l.code match {
      case code if code.toIntOption.isDefined    => s"${Constants.builtinPrefix}int"
      case code if code.toDoubleOption.isDefined => s"${Constants.builtinPrefix}float"
      case "True" | "False"                      => s"${Constants.builtinPrefix}bool"
      case "None"                                => s"${Constants.builtinPrefix}None"
      case code if isPyString(code)              => s"${Constants.builtinPrefix}str"
      case code if code.startsWith("Union[") =>
        val types = code.stripPrefix("Union[").stripSuffix("]").split(",").map(_.trim)
        return types.map(t => s"${Constants.builtinPrefix}$t").toSet
      case _                                     => return Set.empty
    }
    val literalTypes = Set(literalType)
    setTypes(l, literalTypes.toSeq)
    literalTypes
  }

  override def createCallFromIdentifierTypeFullName(typeFullName: String, callName: String): String = {
    lazy val typeName = typeFullName.split("\\.").lastOption.getOrElse(typeFullName)
    typeFullName match {
      case fullName if fullName.matches(".*(<\\w+>)$") =>
        super.createCallFromIdentifierTypeFullName(typeFullName, callName)
      case fullName if fullName.matches(".*\\.<(member|returnValue|indexAccess)>(\\(.*\\))?") =>
        super.createCallFromIdentifierTypeFullName(typeFullName, callName)
      case fullName if isConstructor(typeName) =>
        Seq(fullName, callName).mkString(pathSep)
      case _ =>
        super.createCallFromIdentifierTypeFullName(typeFullName, callName)
    }
  }

  override protected def postSetTypeInformation(): Unit = {
    super.postSetTypeInformation()
    cu.typeDecl
      .map(t => t -> t.inheritsFromTypeFullName.partition(itf => symbolTable.contains(LocalVar(itf))))
      .foreach { case (t, (identifierTypes, otherTypes)) =>
        val existingTypes = (identifierTypes ++ otherTypes).distinct
        val resolvedTypes = identifierTypes.map(LocalVar.apply).flatMap(symbolTable.get)
        if (existingTypes != resolvedTypes && resolvedTypes.nonEmpty) {
          builder.setNodeProperty(t, PropertyNames.INHERITS_FROM_TYPE_FULL_NAME, resolvedTypes)
        }
      }
  }

  override def prepopulateSymbolTable(): Unit = {
    cu.ast.isMethodRef.where(_.astSiblings.isIdentifier.nameExact("classmethod")).referencedMethod.foreach {
      classMethod =>
        classMethod.parameter
          .nameExact("cls")
          .foreach { cls =>
            val clsPath = classMethod.typeDecl.fullName.toSet
            symbolTable.put(LocalVar(cls.name), clsPath)
            if (cls.typeFullName == Constants.ANY)
              builder.setNodeProperty(cls, PropertyNames.DYNAMIC_TYPE_HINT_FULL_NAME, clsPath.toSeq)
          }
    }
    super.prepopulateSymbolTable()
  }

  override protected def visitIdentifierAssignedToTypeRef(i: Identifier, t: TypeRef, rec: Option[String]): Set[String] =
    t.typ.referencedTypeDecl
      .map(_.fullName.stripSuffix("<meta>"))
      .map(td => symbolTable.append(CallAlias(i.name, rec), Set(td)))
      .headOption
      .getOrElse(super.visitIdentifierAssignedToTypeRef(i, t, rec))

  override protected def handlePotentialFunctionPointer(
    funcPtr: Expression,
    baseTypes: Set[String],
    funcName: String,
    baseName: Option[String]
  ): Unit = {
    if (funcName != Constants.moduleName)
      super.handlePotentialFunctionPointer(funcPtr, baseTypes, funcName, baseName)
  }

  override protected def getIndexAccessTypes(ia: Call): Set[String] = {
    ia.argument.argumentIndex(1).isCall.headOption match {
      case Some(c) => getTypesFromCall(c).map(x => s"$x$pathSep${XTypeRecovery.DummyIndexAccess}")
      case _       => super.getIndexAccessTypes(ia)
    }
  }

  def visitIdentifierAssignedToLambda(i: Identifier, l: AstNode): Set[String] = {
    val returnType = inferLambdaReturnType(l)
    associateTypes(i, Set(s"${Constants.builtinPrefix}function[${returnType}]"))
  }

  private def inferLambdaReturnType(l: AstNode): String = {
    l.astChildren.lastOption.flatMap(x => getTypes(x).headOption).getOrElse(Constants.ANY)
  }

}
