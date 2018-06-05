package com.ibm.aardpfark.pfa.functions

import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.spark.ml.linear.LinearModelData


trait FunctionLibrary {

  // core ops
  object core {
    object plus {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("+", arg1, arg2)
    }
    object minus {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("-", arg1, arg2)
    }
    object mult {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("*", arg1, arg2)
    }
    object div {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("/", arg1, arg2)
    }
    object addinv {
      def apply(arg: Any) = new FunctionCall("u-", arg)
    }
    object and {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("&&", arg1, arg2)
    }
    object eq {
      def apply(arg1: PFAExpression, arg2: PFAExpression) = new FunctionCall("==", arg1, arg2)
    }
    object lt {
      def apply(arg1: PFAExpression, arg2: PFAExpression) = new FunctionCall("<", arg1, arg2)
    }
    object lte {
      def apply(arg1: PFAExpression, arg2: PFAExpression) = new FunctionCall("<=", arg1, arg2)
    }
    object gt {
      def apply(arg1: PFAExpression, arg2: PFAExpression) = new FunctionCall(">", arg1, arg2)
    }
    object gte {
      def apply(arg1: PFAExpression, arg2: PFAExpression) = new FunctionCall(">=", arg1, arg2)
    }
    object net {
      def apply(arg1: PFAExpression, arg2: PFAExpression) = new FunctionCall("!=", arg1, arg2)
    }
    object pow {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("**", arg1, arg2)
    }
    object not {
      def apply(arg: Any) = new FunctionCall("!", arg)
    }
  }
  // math
  object m {
    // core math
    object abs {
      def apply(arg: Any) = new FunctionCall("m.abs", arg)
    }
    object exp {
      def apply(arg: Any) = new FunctionCall("m.exp", arg)
    }
    object ln {
      def apply(arg: Any) = new FunctionCall("m.ln", arg)
    }
    object sqrt {
      def apply(arg: Any) = new FunctionCall("m.sqrt", arg)
    }
    // m.link
    object link {
      object cloglog {
        def apply(arg: Any) = new FunctionCall("m.link.cloglog", arg)
      }
      object logit {
        def apply(arg: Any) = new FunctionCall("m.link.logit", arg)
      }
      object probit {
        def apply(arg: Any) = new FunctionCall("m.link.probit", arg)
      }
      object softmax {
        def apply(arg: Any) = new FunctionCall("m.link.softmax", arg)
      }
    }

  }
  object impute {
    object isnan {
      def apply(arg: Any) = new FunctionCall("impute.isnan", arg)
    }
  }
  // string
  object s {
    object lower {
      def apply(arg: PFAExpression) = new FunctionCall("s.lower", arg)
    }
    object join {
      def apply(arg1: Any, arg2: PFAExpression) = new FunctionCall("s.join", arg1, arg2)
    }
    object strip {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("s.strip", arg1, arg2)
    }
  }
  // regex
  object re {
    object split {
      def apply(arg1: PFAExpression, arg2: PFAExpression) = new FunctionCall("re.split", arg1, arg2)
    }
  }
  // arrays
  object a {
    object argmax {
      def apply(arg: Any) = new FunctionCall("a.argmax", arg)
    }
    object append {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("a.append", arg1, arg2)
    }
    object replace {
      def apply(arg1: Any, arg2: Any, arg3: Any) = new FunctionCall("a.replace", arg1, arg2, arg3)
    }
    object contains {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("a.contains", arg1, arg2)
    }
    object filter {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("a.filter", arg1, arg2)
    }
    object filterWithIndex {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("a.filterWithIndex", arg1, arg2)
    }
    object zipmap {
      def apply(a: Any, b: Any, fcn: FunctionRef) = new FunctionCall("a.zipmap", a, b, fcn)
      def apply(a: Any, b: Any, c: Any, fcn: FunctionRef) = new FunctionCall("a.zipmap", a, b, c, fcn)
      def apply(a: Any, b: Any, c: Any, d: Any, fcn: FunctionRef) = new FunctionCall("a.zipmap", a, b, c, d, fcn)
      def apply(fcn: FunctionRef, a: Any*): FunctionCall = {
        a.length match {
          case 2 => apply(a(0), a(1), fcn)
          case 3 => apply(a(0), a(1), a(2), fcn)
          case 4 => apply(a(0), a(1), a(2), a(3), fcn)
          case _ => throw new UnsupportedOperationException("Cannot support this many arguments")
        }
      }
      //TODO Add test

    }
    object flatten {
      def apply(arg: Any) = new FunctionCall("a.flatten", arg)
    }
    object map {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("a.map", arg1, arg2)
    }
    object max {
      def apply(arg: Any) = new FunctionCall("a.max", arg)
    }
    object mode {
      def apply(arg1: Any) = new FunctionCall("a.mode", arg1)
    }
    object mean {
      def apply(arg1: Any) = new FunctionCall("a.mean", arg1)
    }
    object sum {
      def apply(arg1: Any) = new FunctionCall("a.sum", arg1)
    }
    object slidingWindow {
      def apply(arg1: Any, arg2: Any, arg3: Any) = new FunctionCall("a.slidingWindow", arg1, arg2, arg3)
    }
    object len {
      def apply(arg1: Any) = new FunctionCall("a.len", arg1)
    }
    object index {
      def apply(arg1: Any, arg2: FunctionRef) = new FunctionCall("a.index", arg1, arg2)
    }
    object subseq {
      def apply(arg1: Any, arg2: Any, arg3: Any) = new FunctionCall("a.subseq", arg1, arg2, arg3)
    }
  }
  // type casting
  object cast {
    object json {
      def apply(arg: Any) = new FunctionCall("cast.json", arg)
    }
    object int {
      def apply(arg: Any) = new FunctionCall("cast.int", arg)
    }
    object double {
      def apply(arg: Any) = new FunctionCall("cast.double", arg)
    }
  }
  // maps
  object map {
    object add {
      def apply(arg1: Any, arg2: Any, arg3: Any) = new FunctionCall("map.add", arg1, arg2, arg3)
    }
    object containsKey {
      def apply(arg1: Any, arg2: PFAExpression) = new FunctionCall("map.containsKey", arg1, arg2)
    }
    object filter {
      def apply(arg1: Any, arg2: FunctionRef) = new FunctionCall("map.filter", arg1, arg2)
    }
    object map {
      def apply(arg1: Any, arg2: FunctionRef) = new FunctionCall("map.map", arg1, arg2)
    }
    object mapWithKey {
      def apply(arg1: Any, arg2: FunctionRef) = new FunctionCall("map.mapWithKey", arg1, arg2)
    }
    object keys {
      def apply(arg1: Any) = new FunctionCall("map.keys", arg1)
    }
    object values {
      def apply(arg1: Any) = new FunctionCall("map.values", arg1)
    }
    object zipmap {
      def apply(arg1: Any, arg2: Any, arg3: FunctionRef) = new FunctionCall("map.zipmap", arg1, arg2, arg3)
    }
  }
  // models
  object model {
    // regression
    object reg {
      // linear model
      object linear {
        def apply(arg1: Any, arg2: CellRetrieval[LinearModelData]) = {
          new FunctionCall("model.reg.linear", arg1, arg2)
        }
      }
    }
    // clustering
    object cluster {
      object closest {
        def apply(arg1: Any, arg2: Any) = new FunctionCall("model.cluster.closest", arg1, arg2)
      }
    }
    // neural net
    object neural {
      object simpleLayers {
        def apply(arg1: Any, arg2: Any, arg3: FunctionRef) = new FunctionCall("model.neural.simpleLayers", arg1, arg2, arg3)
      }
    }
    // trees
    object tree {
      object simpleWalk {
        def apply(arg1: Any, arg2: Any, arg3: FunctionRef) = new FunctionCall("model.tree.simpleWalk", arg1, arg2, arg3)
      }
    }
  }
  // linear algebra
  object la {
    object add {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("la.add", arg1, arg2)
    }
    object dot {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("la.dot", arg1, arg2)
    }
    object map {
      def apply(arg1: Any, arg2: FunctionRef) = new FunctionCall("la.map", arg1, arg2)
    }
    object scale {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("la.scale", arg1, arg2)
    }
    object sub {
      def apply(arg1: Any, arg2: Any) = new FunctionCall("la.sub", arg1, arg2)
    }
  }

}
