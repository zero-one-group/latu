defmodule Latu.RetryTest do
  use ExUnit.Case, async: true

  alias Latu.Retry

  doctest Latu.Retry

  describe "new/1" do
    test "defaults are PySpark's DefaultPolicy" do
      assert %Retry{
               max_retries: 15,
               initial_backoff: 50,
               max_backoff: 60_000,
               backoff_multiplier: 4.0,
               jitter: 500,
               min_jitter_threshold: 2_000
             } = Retry.new()
    end

    test "one field overridden keeps the rest" do
      retry = Retry.new(max_retries: 2)

      assert retry.max_retries == 2
      assert retry.initial_backoff == 50
    end

    test "a %Retry{} passes through, so connect/2 takes either form" do
      retry = Retry.new(max_retries: 2)

      assert Retry.new(retry) == retry
    end

    test "an unknown field is refused rather than dropped" do
      assert_raise KeyError, fn -> apply(Retry, :new, [[max_retires: 2]]) end
    end

    test "a refusal names the field and what it takes" do
      assert_raise ArgumentError, ~r/^:max_retries is a non-negative integer, not -1$/, fn ->
        apply(Retry, :new, [[max_retries: -1]])
      end

      assert_raise ArgumentError, ~r/^:initial_backoff is a non-negative integer/, fn ->
        apply(Retry, :new, [[initial_backoff: "50"]])
      end

      assert_raise ArgumentError, ~r/^:backoff_multiplier is a number of at least 1/, fn ->
        apply(Retry, :new, [[backoff_multiplier: 0.5]])
      end
    end

    test "anything that is neither is refused by name" do
      assert_raise ArgumentError, ~r/keyword list or a %Latu.Retry\{\}/, fn ->
        apply(Retry, :new, [42])
      end
    end
  end

  describe "wait/2" do
    test "grows by the multiplier and stops at the ceiling" do
      retry = Retry.new(jitter: 0)

      assert Enum.map(0..3, &Retry.wait(retry, &1)) == [50, 200, 800, 3_200]
      assert Retry.wait(retry, 99) == 60_000
    end

    test "jitter applies only above the threshold, and is bounded by it" do
      retry = Retry.new()

      # 800ms is under the 2s threshold, so it is exact; 3200ms is over it.
      assert Retry.wait(retry, 2) == 800

      for _ <- 1..50 do
        wait = Retry.wait(retry, 3)

        assert wait >= 3_200 and wait < 3_700
      end
    end

    test "the schedule is the session's, so a shorter one is a shorter one" do
      retry = Retry.new(initial_backoff: 10, backoff_multiplier: 2, jitter: 0)

      assert Enum.map(0..3, &Retry.wait(retry, &1)) == [10, 20, 40, 80]
    end
  end
end
