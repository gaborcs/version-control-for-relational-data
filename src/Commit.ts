import type { Transaction } from "kysely";
import { v4 as uuidv4 } from "uuid";
import type { VersionControlledSchema } from "./VersionControlledSchema";

export class Commit<BranchMetadata, CommitMetadata, VersionControlledTables> {
  constructor(
    private readonly branchId: number,
    private readonly commitId: number,
    private readonly tx: Transaction<
      VersionControlledSchema<
        BranchMetadata,
        CommitMetadata,
        VersionControlledTables
      >
    >,
  ) {}

  async insert<TableName extends keyof VersionControlledTables & string>(
    tableName: TableName,
    values: VersionControlledTables[TableName],
  ) {
    const id = uuidv4();
    await this.tx
      .insertInto(tableName)
      // @ts-ignore: Kysely doesn't work great with generics
      .values({
        ...values,
        id,
        branch_id: this.branchId,
        valid_from: this.commitId,
      })
      .execute();
    return id;
  }
}
