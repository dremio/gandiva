<!---
Copyright (C) 2017-2018 Dremio Corporation
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
-->

## Gandiva

<table>
  <tr>
    <td>Build Status</td>
    <td>
    <a href="https://travis-ci.com/dremio/gandiva">
    <img src="https://travis-ci.com/dremio/gandiva.svg?token=orr7DJtgDy3dDXRXJZD1&branch=master" alt="travis build status" />
    </a>
    </td>
  </tr>
</table>

### Efficient expression evaluation

Gandiva is a toolset for compiling and evaluating expressions on arrow data. It uses LLVM for doing just-in-time compilation of the expressions.

#### Processing Arrow data

The [Apache Arrow project](https://github.com/apache/arrow/) implements a columnar format for the representation and processing of big-data. It supports both flat and nested types, and has interfaces for memory sharing and transfer to disk/network. Gandiva provides the libraries to process data in arrow format using SQL-like expressions (project, filter and aggregate).

#### Run-time code generation

SQL support a wide range of datatypes and operators. There are multiple ways to evaluate an expression :

 1. Using an interpreter<br>
When processing huge amounts of data, the interpreter becomes an overhead and affects the performance. This technique causes a lot of branching and conditional checks during runtime, which makes it unsuitable to take full advantage of the pipelining and SIMD capabilities of modern CPUs.

  2.  Using runtime code generation<br>
In this method, the code for the SQL expression is generated at runtime and compiled using a JIT compiler. And, the generated code is then used to evaluate the expressions. The performance of expression evaluation is significantly higher in this mode compared to using an interpreter.<br>
Code generation can be done to a higher level language like Java and the the resulting code could be compiled using JIT techniques. However, this involves is an intermediate step of Java byte-code generation which makes it harder for using SIMD optimisations. Also, the limitations on byte-code means that the code generation cannot use wider registers that are common in modern hardware to implement data types like decimals.<br>
Gandiva uses [LLVM](http://llvm.org/) tools to generate IR code and compile it at run-time to take maximum advantage of the hardware capabilities on the target machine. LLVM supports a wide variety of optimizations on the IR code like function inlining, loop vectorization and  instruction combining. Further, it supports the addition of custom optimization passes.

  
#### Code Generation for processing Arrow data

Almost all arrow arrays have two components - a data vector and a bitmap vector. The bitmap vector encodes the validity of the corresponding cell in the data vector. The result of an expression is dependent on the validity of the inputs.

 Consider a simple expression *c = *a + b**. For any given cell, the value of c is *null* if either a is *null* or b is *null*.

  The repeated checking of the validity bit for each input element and for each intermediate output in an expression tree has a deteriorating effect on performance (prevents pipelining).
  
Gandiva takes a novel approach to solving this problem. Wherever possible, it decomposes the computation of the validity and value portions so that the two can be computed independently. Eg. consider the expression :

                         R = A + B - C

  The expression is decomposed into two parts :

1.  Value portion 

            A (data) + B (data) - C (data)
    This part is computed ignoring the validity of the components. This can be implemented as a simple for loop acting on three input vectors. Further, the loop can further be optimized to use SIMD operations.
    

2.  Validity portion : 

            A(validity bit) & B(validity bit) & C (validity bit)
    This part is working only on bitmaps - hence, the result bitmap can be computed efficiently using 64-bit arithmetic.
    
This technique eliminates the validity checks during evaluation and so, is significantly more efficient since it allows for better pipelining of instructions on the CPU.
  
 Similar optimizations can be done for the other operators and expressions to take the advantage of the vector structure of array data, and the techniques available for optimizing LLVM IR.

#### Runtime IR and Pre-compiled IR

LLVM IR can be generated in two ways :

 
1.  Compile time IR generation from C/C++ functions using Clang++
    
2.  Runtime IR generation using IR builder
    
Gandiva uses both of these mechanisms. It uses clang to build a registry of pre-compiled IR modules, called “function registry” The registry has IR code for a lot of known functions eg. extractYear(), isnumeric() etc. 

It also uses the IR builder at runtime to create glue functions that :

-  Loops over the elements in the data vector
-  Embeds code from the corresponding functions in the registry based on the SQL expression
- Implements control statements that are independent of functions like case-statements, if-else statements, boolean-expressions and in-expressions.
    

 The IR code for a set of expressions are generated in an LLVM module, which includes both the glue code generated at runtime and the pre-compiled IR functions. The combined module is then compiled together to allow efficient inlining and cross-optimizations between the runtime IR and pre-compiled functions. The resulting machine code is then used to efficiently evaluate any number of record batches.

### Terminology

#### Expression builder

Gandiva supports a simple tree-based expression builder, that defines the operand, the input types and the output type at each level in the tree. In the future, this can be extended to support other alternatives for building  expressions eg. sql-like statements, post-fix notation, ..

#### Projector

The projector is first instantiated using a vector of expressions built with the expression builder. At this point, the LLVM module is generated, and the generated IR code is optimized and compiled.

Once instantiated, the projector can be used to evaluate any number of incoming record batches.

In the future, support can be added to extend additional constructs like filters and aggregations.

### Sample Code

Here’s the sample C++ code to generate the sum of two integer arrow arrays.

     // schema for input fields
     auto field0 = field("f0", int32());
     auto field1 = field("f2", int32());
     auto schema = arrow::schema({field0, field1});
     
     // output fields
     auto field_sum = field("add", int32());
     
     // Build expression
     auto sum_expr = TreeExprBuilder::MakeExpression("add {field0, field1}, field_sum);
     
     // Build a projector
     std::shared_ptr<Projector> projector;
     Status status = Projector::Make(schema, {sum_expr}, pool_, &projector);
     
     // Add code for building arrow arrays (corresponding to field0 and field1)
     ....
     
     // prepare  input  record  batch
     auto in_batch = arrow::RecordBatch::Make(schema,num_records, {array0, array1});
     
     // Evaluate  expression. The result arrow arrays(s) are returned in ‘outputs’.
     arrow::ArrayVector outputs;
     status = projector->Evaluate(*in_batch, &outputs);

  
### Multi-threading
 Once a projector is built, it can be shared between threads to evaluate record batches in parallel.

However, the actual division of work and queuing work to multiple threads must be managed by the callers. Gandiva does not attempt to solve this.

### Language Support

The core functionality of Gandiva is implemented in C++. Language bindings are provided for Java.

### Performance

To validate the techniques, we did a performance test with DremIO software using two alternative techniques of code generation : using Java code generation vs gandiva.

Two simple expressions were selected and the expression evaluation time alone was compared to process a json file having a 100 million records.


#### Sum
     SELECT max(x+N2x+N3x) FROM testJson."100M.json"

#### CASE-10

    SELECT count
    (case
     when x < 1000000 then 0
     when x < 2000000 then 1
     when x < 3000000 then 2
     when x < 4000000 then 3
     when x < 5000000 then 4
     when x < 6000000 then 5
     when x < 7000000 then 6
     when x < 8000000 then 7
     when x < 9000000 then 8
     when x < 10000000 then 9
     else 10
     end)
     FROM testJson."100M.json"

#### Results

<table>
  <tr>
    <td>
       Test
    </td>
    <td>
       Project time with Java JIT
    </td>
    <td>
        Project time with LLVM
     </td>
     <td>
        Performance improvement with LLVM
     </td>
  </tr>
    <tr>
    <td>
       SUM
    </td>
    <td>
       TODO
    </td>
    <td>
        TODO
     </td>
     <td>
        TODO
     </td>
  </tr>
</tr>
    <tr>
    <td>
       CASE-10
    </td>
    <td>
       TODO
    </td>
    <td>
        TODO
     </td>
     <td>
        TODO
     </td>
  </tr>
  </tr>
    <tr>
    <td>
       CASE-20
    </td>
    <td>
       TODO
    </td>
    <td>
        TODO
     </td>
     <td>
        TODO
     </td>
  </tr>
  </tr>
    <tr>
    <td>
       CASE-40
    </td>
    <td>
       TODO
    </td>
    <td>
        TODO
     </td>
     <td>
        TODO
     </td>
  </tr>
</table>
