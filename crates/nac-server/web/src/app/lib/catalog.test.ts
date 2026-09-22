import { expect, it } from "vitest";

import { catalogProviderForModel } from "@/app/lib/catalog";
import type { CatalogProvider, ModelCatalog } from "@/app/types/api";

function openAiProvider(id: "openai-responses" | "openai-chat-completions"): CatalogProvider {
  return {
    id,
    auth: "api_key_env",
    auth_status: "ready",
    auth_hint: null,
    connection: { base_url: "https://api.openai.com/v1", api_key_env: "OPENAI_API_KEY" },
    default_base_url: "https://api.openai.com/v1",
    managed_base_url: null,
    default_limits: { context_window: 128_000, max_tokens: 16_384, supported_efforts: [] },
    models: [
      {
        id: "gpt-shared",
        display_name: "Shared OpenAI model",
        context_window: 128_000,
        max_tokens: 16_384,
        cost: { input: 0, output: 0, cache_read: 0, cache_write: 0 },
        reasoning: false,
        supported_efforts: [],
        source: "baseline",
      },
    ],
  };
}

it("keeps model-only OpenAI inference on Responses when both protocols contain the id", () => {
  const catalog = {
    catalog_version: 1,
    providers: [openAiProvider("openai-chat-completions"), openAiProvider("openai-responses")],
  } as ModelCatalog;

  expect(catalogProviderForModel(catalog, "gpt-shared")).toBe("openai-responses");
});
