### Description

This is the TaskScheduler with priority support.

The using Microsoft.Extensions.Logging is commented out because it would require DI support.
The Akka Logging system is not suiteable to use, because it would generate Task on the Scheduler itself.

Priorities:
High => all work items will be executed until none left
Normal => work items will be executed until max-work
Low => when work was done then single work items will be executed
        else work items will be executed until max-work
Idle => A single work item will be executed only when no work was done before

### Shortcomings

If the first task is a long running task and it is not scheduled with others,
it could delay the start of the shortly after queued task up to work-interval / work-step milliseconds.

It is working with 1.4.21 well.
Because of some improvments in 1.4.22 with Ask, a deathlock in Akka.Cluster startup got visible
Maybe it got fixed with 1.4.24 (not tested)

### Usage

Add to the akka config:
```
akka.channel-scheduler {
    parallelism-min = 4    #same as for ForkJoinDispatcher
    parallelism-factor = 1 #same as for ForkJoinDispatcher
    parallelism-max = 64   #same as for ForkJoinDispatcher
    work-max = 10          #max executed work items in sequence until priority loop
	work-interval = 500    #time target of executed work items in ms
	work-step = 2          #target work item count in interval / burst
}

akka.actor.default-dispatcher = {
    executor = "Akka.Experimental.ChannelTaskScheduler.ChannelExecutorConfigurator,Akka.Experimental.ChannelTaskScheduler"
    channel-executor.priority = "normal"
}

akka.actor.internal-dispatcher = {
    executor = "Akka.Experimental.ChannelTaskScheduler.ChannelExecutorConfigurator,Akka.Experimental.ChannelTaskScheduler"   
    throughput = 5
    channel-executor.priority = "high"
}

akka.remote.default-remote-dispatcher {
    type = Dispatcher
    executor = "Akka.Experimental.ChannelTaskScheduler.ChannelExecutorConfigurator,Akka.Experimental.ChannelTaskScheduler"
    channel-executor.priority = "high"
}

akka.remote.backoff-remote-dispatcher {
	executor = "Akka.Experimental.ChannelTaskScheduler.ChannelExecutorConfigurator,Akka.Experimental.ChannelTaskScheduler"
	channel-executor.priority = "low"
}
```

The Scheduler is not realy on akka itself dependend, 
The Dotnet TaskSchduler can be accessed and used (thread-safe) with:
```
ChannelTaskScheduler.Get(system).High
ChannelTaskScheduler.Get(system).Normal
ChannelTaskScheduler.Get(system).Low
ChannelTaskScheduler.Get(system).Idle
```
