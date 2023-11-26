# Programming Assignment 3

## Table of Contents

- [Design Decisions](#design-decisions)
- [API Changes](#api-changes)
- [Missing or Incomplete Code](#missing-or-incomplete-code)
- [Time and Challenges](#time-and-challenges)
- [Collaboration](#collaboration)

## Design Decisions
- *(Describe any design decisions you made.)*
- **Filter**: To fetchNext, it iterates over tuples from the child iterator and applies the predicate to filter data.
- **Join**: It's a nested loop join method. It iterates over tuples from both child iterators, applies the join predicate, and constructs a new tuple that merges fields from both input tuples when the predicate is satisfied.
- **Insert and Deletion**:  This operator efficiently implements insert and delete queries by either inserting new tuples into a specified table using the BufferPool's insertTuple method, or deleting existing tuples from the same table using the BufferPool's deleteTuple method, based on the tuples it receives from its child operator.

## API Changes
- *(Discuss and justify any changes you made to the API.)*
- **Status**: No changes made to the API.

## Missing or Incomplete Code
- *(Describe any missing or incomplete elements of your code.)*
- **Status**: All requirements in PA3 have been completed.

## Time and Challenges
- *(Describe how long you spent on the assignment, and whether there was anything you found particularly difficult or confusing.)*
- **Duration**: 
- **Challenges**: The simple nested loop join method used in join function, while straightforward, can be inefficient for large datasets, as it iterates over every tuple pair, leading to higher computational costs.

## Collaboration
- *(Describe how you split the workload.)*
- **Workload Distribution**: Di handled exercises 1 and 3, while Chen was responsible for exercise 2.