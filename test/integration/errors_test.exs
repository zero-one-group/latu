defmodule Latu.Integration.ErrorsTest do
  use ExUnit.Case, async: true

  # Needs a Spark Connect server on :15003 — docker compose up -d spark-reattach.
  #
  # What is a question rather than a smoke test:
  #
  #   * whether the server attaches ErrorInfo at all (PySpark depends on it, so this should
  #     hold, but Latu has never read the trailers before);
  #   * whether `stackTrace` is in the metadata — that is a server conf, so nothing here
  #     asserts it is present, only that if it is, it is a string;
  #   * whether an *analysis* error carries a cause chain at all. A refused plan may have
  #     nothing to chain, which is why the chain is exercised against a runtime failure.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15003"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)

    %{session: session}
  end

  describe "what arrives with the failure, at no extra cost" do
    setup %{session: session} do
      {:error, error} = session |> Latu.range(5) |> Latu.select(:nope) |> Latu.collect()

      %{error: error}
    end

    test "the Spark error class, which is what the docs are indexed by", %{error: error} do
      assert error.error_class =~ "UNRESOLVED_COLUMN"
    end

    test "the SQLSTATE", %{error: error} do
      assert is_binary(error.sql_state)
      assert error.sql_state != ""
    end

    test "the JVM class hierarchy, most specific first", %{error: error} do
      assert [most_specific | _] = error.classes
      assert most_specific =~ "AnalysisException"
    end

    test "the message parameters, so you need not parse the sentence", %{error: error} do
      assert is_map(error.parameters)
      assert error.parameters["objectName"] =~ "nope"
    end

    test "an error id, which is the handle error_details/2 uses", %{error: error} do
      assert is_binary(error.error_id)
    end

    test "a stack trace if the server was configured to send one", %{error: error} do
      assert is_nil(error.stacktrace) or is_binary(error.stacktrace)
    end

    test "and the message still reads as Spark wrote it", %{error: error} do
      # The class is already in Spark's own message, so message/1 must not print it twice.
      rendered = Exception.message(error)

      assert rendered == error.message
      assert length(String.split(rendered, error.error_class)) == 2
    end
  end

  describe "error_details/2" do
    test "fills the cause chain for a runtime failure", %{session: session} do
      {:error, error} =
        session
        |> Latu.range(5)
        |> Latu.select(bad: Latu.Column.expr("1 / 0 * id"))
        |> Latu.filter(Latu.Column.expr("assert_true(id < 0)"))
        |> Latu.collect()

      assert {:ok, filled} = Latu.error_details(session, error)

      # The claim is the chain, not any particular exception in it: which JVM exceptions Spark
      # nests is its business, and it changes between releases.
      assert [_ | _] = filled.causes
      assert Enum.all?(filled.causes, &is_binary(&1.message))
      assert Enum.all?(filled.causes, &is_list(&1.stacktrace))
    end

    test "the original error is unchanged apart from the causes", %{session: session} do
      {:error, error} = session |> Latu.range(5) |> Latu.select(:nope) |> Latu.collect()

      assert {:ok, filled} = Latu.error_details(session, error)

      assert filled.message == error.message
      assert filled.error_class == error.error_class
    end

    # The server invalidates an error id as it answers, so the detail is available once. Latu
    # keeps what it has rather than emptying the field on the second call.
    test "asking twice keeps the causes from the first answer", %{session: session} do
      {:error, error} =
        session
        |> Latu.range(5)
        |> Latu.filter(Latu.Column.expr("assert_true(id < 0)"))
        |> Latu.collect()

      assert {:ok, once} = Latu.error_details(session, error)
      assert {:ok, twice} = Latu.error_details(session, once)

      assert twice.causes == once.causes
    end

    test "an error the server never sent comes back untouched, not as a failure", %{
      session: session
    } do
      local = Latu.Error.new(:decode, "expected one column, got 3")

      assert {:ok, ^local} = Latu.error_details(session, local)
    end
  end
end
