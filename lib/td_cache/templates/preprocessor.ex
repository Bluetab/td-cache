defmodule TdCache.Templates.Preprocessor do
  @moduledoc false

  alias TdCache.Templates.AclLoader
  alias TdCache.Templates.FieldFormatter

  def preprocess_template(template, context \\ %{})

  def preprocess_template(template, %{domain_ids: [domain_id | _] = domain_ids} = context)
      when is_integer(domain_id) do
    user_roles = AclLoader.get_roles_and_users(domain_ids)
    user_group_roles = AclLoader.get_roles_and_groups(domain_ids)

    context = context
    |> Map.put(:user_roles, user_roles)
    |> Map.put(:user_group_roles, user_group_roles)

    preprocess_template_content(template, context)
  end

  def preprocess_template(template, context) do
    preprocess_template_content(template, context)
  end

  defp preprocess_template_content(%{content: content} = template, context) do
    content =
      Enum.map(content, fn %{"fields" => fields} = group ->
        fields = Enum.map(fields, &FieldFormatter.format(&1, context))
        %{group | "fields" => fields}
      end)

    %{template | content: content}
  end
end
