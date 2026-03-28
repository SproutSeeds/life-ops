import { composeAgenda } from "./agenda.js";
import { normalizeItems } from "./model.js";

function stampConnectorItems(connectorName, items = []) {
  return normalizeItems(
    items.map((item) => {
      if (!item || typeof item !== "object") {
        return item;
      }
      const rawSource = item.source && typeof item.source === "object" ? item.source : {};
      const stampedConnector = String(rawSource.connector ?? "").trim();
      return {
        ...item,
        source: {
          ...rawSource,
          connector: stampedConnector && stampedConnector !== "manual" ? stampedConnector : connectorName,
        },
      };
    }),
  );
}

function validateConnectorShape(connector) {
  if (!connector || typeof connector !== "object") {
    throw new Error("Expected a connector definition object.");
  }
  const name = String(connector.name ?? "").trim();
  if (!name) {
    throw new Error("Connectors require a name.");
  }
  if (typeof connector.pull !== "function") {
    throw new Error(`Connector "${name}" must define an async pull(context) method.`);
  }
  return {
    name,
    description: connector.description ? String(connector.description).trim() : "",
    pull: connector.pull,
  };
}

export function defineConnector(connector) {
  return validateConnectorShape(connector);
}

export function createStaticConnector({ name, description = "", items = [], meta = {} }) {
  return defineConnector({
    name,
    description,
    async pull() {
      return {
        items: stampConnectorItems(name, items),
        meta,
      };
    },
  });
}

export async function collectItems({
  connectors = [],
  context = {},
  continueOnError = true,
} = {}) {
  const reports = [];
  const mergedItems = [];
  const dedupe = new Map();

  for (const rawConnector of connectors) {
    const connector = defineConnector(rawConnector);
    try {
      const result = await connector.pull({ ...context, connectorName: connector.name });
      const items = stampConnectorItems(connector.name, result?.items ?? []);
      for (const item of items) {
        const key = `${item.source.connector}:${item.id}`;
        dedupe.set(key, item);
      }
      reports.push({
        name: connector.name,
        ok: true,
        itemCount: items.length,
        meta: result?.meta ?? {},
        errors: [],
      });
    } catch (error) {
      if (!continueOnError) {
        throw error;
      }
      reports.push({
        name: connector.name,
        ok: false,
        itemCount: 0,
        meta: {},
        errors: [error instanceof Error ? error.message : String(error)],
      });
    }
  }

  mergedItems.push(...dedupe.values());
  return {
    collectedAt: new Date().toISOString(),
    items: mergedItems,
    reports,
  };
}

export class LifeOpsClient {
  constructor({ connectors = [] } = {}) {
    this.connectors = connectors.map((connector) => defineConnector(connector));
  }

  register(connector) {
    this.connectors.push(defineConnector(connector));
    return this;
  }

  async collect(context = {}) {
    return collectItems({
      connectors: this.connectors,
      context,
    });
  }

  async agenda(options = {}) {
    const collection = await this.collect(options.context ?? {});
    return {
      ...composeAgenda({
        items: collection.items,
        now: options.now,
        days: options.days,
        timeZone: options.timeZone ?? options.timezone,
        includeStatuses: options.includeStatuses,
        includeUntimed: options.includeUntimed,
      }),
      connectorReports: collection.reports,
    };
  }
}
