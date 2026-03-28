export type LifeOpsItemKind =
  | "event"
  | "communication"
  | "routine"
  | "task"
  | "alert"
  | "document";

export type LifeOpsItemStatus =
  | "open"
  | "in_progress"
  | "reference"
  | "done"
  | "ignored";

export type LifeOpsPriority = "urgent" | "high" | "normal" | "low";

export interface LifeOpsLink {
  label: string;
  url: string;
}

export interface LifeOpsSource {
  connector?: string;
  id?: string | null;
  account?: string | null;
}

export interface LifeOpsItemInput {
  id?: string;
  kind: LifeOpsItemKind;
  title: string;
  summary?: string;
  status?: LifeOpsItemStatus;
  priority?: LifeOpsPriority;
  startsAt?: string | Date | number;
  endsAt?: string | Date | number;
  dueAt?: string | Date | number;
  followUpAt?: string | Date | number;
  organization?: string | null;
  tags?: string[];
  links?: LifeOpsLink[];
  source?: LifeOpsSource;
  metadata?: Record<string, unknown>;
}

export interface LifeOpsItem {
  id: string;
  kind: LifeOpsItemKind;
  title: string;
  summary: string;
  status: LifeOpsItemStatus;
  priority: LifeOpsPriority;
  startsAt: string | null;
  endsAt: string | null;
  dueAt: string | null;
  followUpAt: string | null;
  organization: string | null;
  tags: string[];
  links: LifeOpsLink[];
  source: Required<LifeOpsSource>;
  metadata: Record<string, unknown>;
}

export interface LifeOpsConnectorResult {
  items?: LifeOpsItemInput[];
  meta?: Record<string, unknown>;
}

export interface LifeOpsConnector {
  name: string;
  description?: string;
  pull(context: Record<string, unknown>): Promise<LifeOpsConnectorResult> | LifeOpsConnectorResult;
}

export interface ConnectorReport {
  name: string;
  ok: boolean;
  itemCount: number;
  meta: Record<string, unknown>;
  errors: string[];
}

export interface AgendaEntry extends LifeOpsItem {
  primaryTime: string | null;
  timeLabel: string | null;
}

export interface AgendaDay {
  date: string;
  label: string;
  items: AgendaEntry[];
}

export interface AgendaResult {
  generatedAt: string;
  windowStart: string;
  windowEnd: string;
  timeZone: string;
  stats: {
    totalItems: number;
    scheduledItems: number;
    floatingItems: number;
    countsByKind: Record<string, number>;
  };
  days: AgendaDay[];
  floatingItems: AgendaEntry[];
  connectorReports?: ConnectorReport[];
}

export interface EmailRecipient {
  email: string;
  name?: string | null;
}

export interface EmailSection {
  heading?: string;
  body?: string;
  bullets?: string[];
}

export interface StructuredEmailDraft {
  to: EmailRecipient[];
  cc: EmailRecipient[];
  bcc: EmailRecipient[];
  subject: string;
  previewText: string;
  intro: string;
  sections: Array<Required<EmailSection>>;
  cta: string;
  closing: string;
  metadata: Record<string, unknown>;
  text: string;
  html: string;
}

export interface EmailSender {
  send(payload: StructuredEmailDraft & { context?: Record<string, unknown> }): Promise<unknown> | unknown;
}

export interface ProjectLink {
  label: string;
  url: string;
}

export interface ProjectDefinition {
  name: string;
  summary: string;
  whyNow?: string;
  highlights?: string[];
  proofPoints?: string[];
  asks?: string[];
  links?: ProjectLink[];
  codebases?: string[];
}

export interface ProjectRecipient {
  email: string;
  name?: string | null;
  whyRecipient?: string;
  ask?: string;
  organization?: string | null;
  subjectHook?: string;
  followUpDays?: number | null;
}

export interface ProjectShareDraft extends StructuredEmailDraft {
  project: ProjectDefinition;
  recipient: Required<ProjectRecipient>;
}

export declare const itemKinds: readonly LifeOpsItemKind[];
export declare const itemStatuses: readonly LifeOpsItemStatus[];
export declare const priorityLevels: readonly LifeOpsPriority[];

export declare function normalizeItem(item: LifeOpsItemInput): LifeOpsItem;
export declare function normalizeItems(items?: LifeOpsItemInput[]): LifeOpsItem[];
export declare function getItemTimestamp(item: LifeOpsItem): string | null;
export declare function compareItems(left: LifeOpsItem, right: LifeOpsItem): number;

export declare function composeAgenda(options?: {
  items?: LifeOpsItemInput[];
  now?: string | Date | number;
  days?: number;
  timeZone?: string;
  timezone?: string;
  includeStatuses?: LifeOpsItemStatus[];
  includeUntimed?: boolean;
}): AgendaResult;

export declare function renderAgendaText(agenda: AgendaResult): string;

export declare function defineConnector(connector: LifeOpsConnector): LifeOpsConnector;
export declare function createStaticConnector(options: {
  name: string;
  description?: string;
  items?: LifeOpsItemInput[];
  meta?: Record<string, unknown>;
}): LifeOpsConnector;

export declare function collectItems(options?: {
  connectors?: LifeOpsConnector[];
  context?: Record<string, unknown>;
  continueOnError?: boolean;
}): Promise<{
  collectedAt: string;
  items: LifeOpsItem[];
  reports: ConnectorReport[];
}>;

export declare class LifeOpsClient {
  constructor(options?: { connectors?: LifeOpsConnector[] });
  connectors: LifeOpsConnector[];
  register(connector: LifeOpsConnector): LifeOpsClient;
  collect(context?: Record<string, unknown>): Promise<{
    collectedAt: string;
    items: LifeOpsItem[];
    reports: ConnectorReport[];
  }>;
  agenda(options?: {
    context?: Record<string, unknown>;
    now?: string | Date | number;
    days?: number;
    timeZone?: string;
    timezone?: string;
    includeStatuses?: LifeOpsItemStatus[];
    includeUntimed?: boolean;
  }): Promise<AgendaResult>;
}

export declare function draftStructuredEmail(options: {
  to?: Array<EmailRecipient | string>;
  cc?: Array<EmailRecipient | string>;
  bcc?: Array<EmailRecipient | string>;
  subject: string;
  previewText?: string;
  intro?: string;
  sections?: EmailSection[];
  cta?: string;
  closing?: string;
  metadata?: Record<string, unknown>;
}): StructuredEmailDraft;

export declare function renderEmailText(draft: StructuredEmailDraft): string;
export declare function renderEmailHtml(draft: StructuredEmailDraft): string;
export declare function sendEmailDraft(options: {
  draft: StructuredEmailDraft;
  sender: EmailSender;
  context?: Record<string, unknown>;
}): Promise<unknown>;

export declare function draftProjectShareEmail(options: {
  project: ProjectDefinition;
  recipient: ProjectRecipient;
  senderName?: string;
  closingNote?: string;
}): ProjectShareDraft;

export declare function createProjectShareFollowUpItem(options: {
  draft: ProjectShareDraft;
  followUpAt?: string | Date | number;
  baseTime?: string | Date | number;
}): LifeOpsItem;

export declare function buildProjectSharePacket(options: {
  project: ProjectDefinition;
  recipients: ProjectRecipient[];
  senderName?: string;
  baseTime?: string | Date | number;
}): {
  project: ProjectDefinition;
  drafts: ProjectShareDraft[];
  followUps: LifeOpsItem[];
};
