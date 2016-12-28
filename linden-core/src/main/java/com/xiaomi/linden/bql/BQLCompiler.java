// Copyright 2016 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.xiaomi.linden.bql;

import com.xiaomi.linden.thrift.common.LindenRequest;
import com.xiaomi.linden.thrift.common.LindenSchema;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.IntegerStack;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayDeque;
import java.util.Deque;

public class BQLCompiler {
  private final LindenSchema lindenSchema;

  public BQLCompiler(LindenSchema lindenSchema) {
    this.lindenSchema = lindenSchema;
  }

  public LindenRequest compile(String bqlStmt) {
    // Lexer splits input into tokens
    ANTLRInputStream input = new ANTLRInputStream(bqlStmt);
    BQLLexer lexer = new BQLLexer(input);
    lexer.removeErrorListeners();
    TokenStream tokens = new CommonTokenStream(lexer);

    // Parser generates abstract syntax tree
    BQLParser parser = new BQLParser(tokens);
    parser.removeErrorListeners();
    BQLCompilerAnalyzer analyzer = new BQLCompilerAnalyzer(parser, lindenSchema);

    /*You can save a great deal of time on correct inputs by using a two-stage parsing strategy.
      1. Attempt to parse the input using BailErrorStrategy and PredictionMode.SLL.
         If no exception is thrown, you know the answer is correct.
      2. If a ParseCancellationException is thrown, retry the parse using the
         default settings (DefaultErrorStrategy and PredictionMode.LL).
    */
    parser.setErrorHandler(new BailErrorStrategy());
    parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
    try {
      BQLParser.StatementContext ret = parser.statement();
      ParseTreeWalker.DEFAULT.walk(analyzer, ret);
      return analyzer.getLindenRequest(ret);
    } catch (ParseCancellationException e) {
      try {
        parser.reset();
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        BQLParser.StatementContext ret = parser.statement();
        ParseTreeWalker.DEFAULT.walk(analyzer, ret);
        return analyzer.getLindenRequest(ret);
      } catch (Exception e2) {
        throw new RuntimeException(getErrorMessage(e2));
      }
    } catch (Exception e) {
      throw new RuntimeException(getErrorMessage(e));
    }
  }

  public String getErrorMessage(Exception error) {
    if (error instanceof NoViableAltException) {
      return getErrorMessage((NoViableAltException) error);
    } else if (error instanceof ParseCancellationException) {
      return getErrorMessage((ParseCancellationException) error);
    } else {
      return error.getMessage();
    }
  }

  protected String getErrorMessage(NoViableAltException error) {
    return String.format("[line:%d, col:%d] No viable alternative (token=%s)",
        error.getOffendingToken().getLine(),
        error.getOffendingToken().getCharPositionInLine(),
        error.getOffendingToken().getText());
  }

  public String getErrorMessage(ParseCancellationException error) {
    if (error.getCause() != null) {
      String message = error.getCause().getMessage();
      if (error.getCause() instanceof SemanticException) {
        SemanticException semanticException = (SemanticException) error.getCause();
        if (semanticException.getNode() != null) {
          TerminalNode startNode = getStartNode(semanticException.getNode());
          if (startNode != null) {
            String prefix = String.format("[line:%d, col:%d] ", startNode.getSymbol().getLine(),
                startNode.getSymbol().getCharPositionInLine());
            message = prefix + message;
          }
        }
        return message;
      } else if (error.getCause() instanceof RecognitionException) {
        return getErrorMessage((RecognitionException) error.getCause());
      } else {
        return error.getCause().getMessage();
      }
    }

    return error.getMessage();
  }

  private static TerminalNode getStartNode(ParseTree tree) {
    if (tree instanceof TerminalNode) {
      return (TerminalNode) tree;
    }

    Deque<ParseTree> workList = new ArrayDeque<ParseTree>();
    IntegerStack workIndexStack = new IntegerStack();
    workList.push(tree);
    workIndexStack.push(0);
    while (!workList.isEmpty()) {
      ParseTree currentTree = workList.peek();
      int currentIndex = workIndexStack.peek();
      if (currentIndex == currentTree.getChildCount()) {
        workList.pop();
        workIndexStack.pop();
        continue;
      }

      // move work list to next child
      workIndexStack.push(workIndexStack.pop() + 1);

      // process the current child
      ParseTree child = currentTree.getChild(currentIndex);
      if (child instanceof TerminalNode) {
        return (TerminalNode) child;
      }

      workList.push(child);
      workIndexStack.push(0);
    }

    return null;
  }
}
