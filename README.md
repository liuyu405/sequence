Sequence
========

不使用Oracle作为数据存储了，却还在怀念Oracle的Sequence？
想要一个集群应用时唯一，不断递增的数字生成？
还想指定区间段，达到区间段的最大值之后再从最小值开始？

不用犹豫，就是这了。
提供以MySQL作为存储介质的序列号生成器实现，提供集群间唯一序列号。如果担心单一的MySQL存储不够稳定，可以提供多个MySQL实例，只有不是所有的MySQL实例都不可使用，就可以使用正常的MySQL实例生成序列号。
第一次分配序列号时向MySQL申请一个序列号段(大小由step设定)，从区间的第一个值开始分配，分配完该段序列号之后，再向MySQL申请下一个序列号段进行分配。同时，通过设置最小值、最大值的方式，来保证生成的序列号处在该区间范围内。