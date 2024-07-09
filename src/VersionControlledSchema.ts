import type { Generated } from "kysely";

export type VersionControlledSchema<
  BranchMetadata,
  CommitMetadata,
  VersionControlledTables,
> = {
  branch: { id: Generated<number> } & BranchMetadata;
  commit: { id: Generated<number> } & CommitMetadata;
} & {
  [TableName in keyof VersionControlledTables]: VersionControlledTables[TableName] & {
    id: string;
    branch_id: number;
    valid_from: number;
    valid_until?: number;
  };
};
