defmodule Latu.ErrorTest do
  use ExUnit.Case, async: true

  doctest Latu.Error

  alias Latu.Error

  describe "message/1" do
    test "a plain error is its message" do
      assert Exception.message(Error.new(:decode, "expected one column, got 3")) ==
               "expected one column, got 3"
    end

    # Spark's own messages already read `[CLASS] text SQLSTATE: xxx`, so prefixing the class
    # again would print it twice. This is the common case for anything from the server.
    test "a class Spark already put in the message is not repeated" do
      message = "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column `x` cannot be resolved."

      error = Error.new(:rpc, message, error_class: "UNRESOLVED_COLUMN.WITH_SUGGESTION")

      assert Exception.message(error) == message
    end

    test "a class Spark did not put in the message is prefixed" do
      error = Error.new(:rpc, "something went wrong", error_class: "SOME_CLASS")

      assert Exception.message(error) == "[SOME_CLASS] something went wrong"
    end

    # A JVM trace is not what anyone wants in a REPL, and it is one field away when they do.
    test "the stack trace is never in the message" do
      error = Error.new(:rpc, "boom", error_class: "X", stacktrace: "at org.apache.spark.Thing")

      refute Exception.message(error) =~ "org.apache.spark"
      assert error.stacktrace == "at org.apache.spark.Thing"
    end

    test "raising one prints the rendered message" do
      error = Error.new(:rpc, "boom", error_class: "SOME_CLASS")

      assert_raise Latu.Error, "[SOME_CLASS] boom", fn -> raise error end
    end
  end

  describe "new/3" do
    test "the structured fields default to empty rather than nil where they are collections" do
      error = Error.new(:decode, "nope")

      assert error.classes == []
      assert error.parameters == %{}
      assert error.causes == []
      assert error.error_class == nil
    end

    test "an unknown option is a mistake, not a silently dropped field" do
      assert_raise KeyError, fn -> apply(Error, :new, [:rpc, "boom", [errorClass: "X"]]) end
    end
  end
end
