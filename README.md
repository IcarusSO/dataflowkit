# dataflowkit

A framework for data scientist and engineering collaboration. 


![alt tag](https://raw.githubusercontent.com/IcarusSO/dataflowkit/master/img/dataflow-design.png)

Data scientists and engineers have different skill sets. While data scientists focus on algorithms and probability accuracy,
engineers focus on data storage and maintance.
This is a framwork heavily borrowed the idea from functional programming and other data flow management tools such as SAS.


A program is decoupled into different individuals. With the aid of a design diagram, both parties and easily understand the flow 
and how to integrate. Following the best practise can greatly shorten the time of development.


Different from other data flow management tools, dataflowkit focus on individual dataflow than batch dataflow.


#### General Idea
- A program is decoupled into Recipes and Datasets. 
- Recipe is the calculation component which provide one public method execute(ins, outs)
- Dataset is the storage component which provide two public methods save(data), load()
- Datasets can be InMemory, S3, Local, MySql and others
- Recipes and Datasets are linking to each other and dataframe or dataframe formatable dict should be the format for data transfer.
- No cyclic flow is allowed
- Components and be replaced such that code refactoring is easily done
- Design can be improved and integrated since it follows functional design (components can be merged or split)


#### General Workflow
1. Dataflow Analysis - figure out the key components (Recipe and Dataset) for the data flow
2. Dataflow Design - figure out all components for the data flow
3. Implementation - implement the algorithms
4. Code Refactoring - do the code refactoring to increase to readability and maintainability


#### Responsibilities
Teams have different sizes and members have differnt skill sets. 
It is recommended each team discuss the responsibilities according to members skill set in the begining clearly.
In general, data scientist focus on algorithm implementations and engineers focus on code refactoring.
Design is a task which both parties should be involved. 
It is very important to design before implement and keep the data flow graph updated.


#### Class Diagram
![alt tag](https://raw.githubusercontent.com/IcarusSO/dataflowkit/master/img/class-diagram.png)


#### Future Development
A webbased interface for data flow design and code generation will be the next task.
You are very welcomed to join us and contribute.

Author: icarus.so.ch@gmail.com


