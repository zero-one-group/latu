# Livebook rendering, behind an optional dependency: without :kino this file compiles to
# nothing and a non-Livebook user pays nothing for it. The HTML is Spark's own — HtmlString is
# the server-side _repr_html_ — so this is a pass-through, not a renderer.
#
# A Session and a GroupedData deliberately have no impl: Kino falls back to inspect/2 for them,
# which is what they should show.
if Code.ensure_loaded?(Kino.Render) do
  defimpl Kino.Render, for: Latu.DataFrame do
    def to_livebook(df) do
      case Latu.DataFrame.to_html(df) do
        {:ok, html} -> Kino.Render.to_livebook(Kino.HTML.new(html))
        {:error, error} -> Kino.Render.to_livebook(error)
      end
    end
  end
end
