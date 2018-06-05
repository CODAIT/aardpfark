package com.ibm.aardpfark.pfa

import com.ibm.aardpfark.pfa.dsl.{ErrorExpr, ExprSeq}

package object expression {

  private[pfa] trait API extends AttributeRetrieval
    with Casts
    with ControlStructures
    with FunctionRefs
    with LetSet
    with Loops
    with New {

    object Action {
      def apply(expr: PFAExpression) = ExprSeq(Seq(expr))
      def apply(first: PFAExpression, next: PFAExpression*) = ExprSeq(Seq(first) ++ next)
    }

    // error messages
    object Error {
      def apply(msg: String) = new ErrorExpr(msg)
    }

  }

}
