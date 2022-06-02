-module(scheduler).
-author("shendatian").
%%================
%% 调度器使用
%%================
%% API
-export([start/0]).

%% @doc 启动入口
start() ->
  %% 派生一个erl进程实现任务，这里可以批量实现，从而实现高并发，最简单的实现方式就是循环spawn.
  %% 顺便提一句，erlang的otp监控机制非常强大，超时重启策略很完善独特。但这里没实现，见谅，详情可以查supervisor模块了解。
  spawn(fun() -> do() end),
  ok.


do() ->
  %% 给进程注册一个名字
  Pid = self(),
  register(scheduler, Pid),
  init(),
  %% 接受最新任务
  do_receive(),
  ok.

do_receive() ->
  receive
    {create} ->
      do_create();
    {read} ->
      do_read();
    {update} ->
      do_update();
    {delete} ->
      do_delete()
  end.

init() ->
  Task = random_task(),
  erlang:send_after(3000, self(), Task).

random(N) ->
  rand:seed(exs1024),
  rand:uniform(N).

%% @doc 随机任务生成器，1-4对应4个作业,提供虚拟任务
random_task() ->
  case random(4) of
    1 -> {create};
    2 -> {read};
    3 -> {update};
    4 -> {delete}
  end.

%% @doc 任务都是虚拟化的，可以在这里实现mysql，mongodb对应的操作。
do_create() ->
  %% todo mysql insert
  %% todo mongodb insert
  %% 这里打印执行的作业，必要时可以输出或者提交完成时间。
  io:format("do create ~n"),
  Task = random_task(),
  erlang:send_after(3000, self(), Task),
  do_receive(),
  ok.


do_read() ->
  %% todo read
  io:format("do read ~n"),
  Task = random_task(),
  erlang:send_after(3000, self(), Task),
  do_receive(),
  ok.

do_update() ->
  %% todo update
  io:format("do update ~n"),
  Task = random_task(),
  erlang:send_after(3000, self(), Task),
  do_receive(),
  ok.

do_delete() ->
  %% to delete
  io:format("do delete ~n"),
  Task = random_task(),
  erlang:send_after(3000, self(), Task),
  do_receive(),
  ok.






