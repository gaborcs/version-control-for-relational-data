import assert from "node:assert";
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

  get id() {
    return this.commitId;
  }

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

  async update<TableName extends keyof VersionControlledTables & string>(
    tableName: TableName,
    id: string,
    values: Partial<VersionControlledTables[TableName]>,
  ) {
    const updatedRows = await this.tx
      .updateTable(tableName)
      // @ts-ignore: Kysely doesn't work great with generics
      .where("id", "=", id)
      // @ts-ignore: Kysely doesn't work great with generics
      .where("branch_id", "=", this.branchId)
      .where("valid_until", "is", null)
      // @ts-ignore: Kysely doesn't work great with generics
      .set({ valid_until: this.commitId })
      .returningAll()
      .execute();
    assert(updatedRows.length === 1, "Expected to update exactly one row");
    await this.tx
      .insertInto(tableName)
      // @ts-ignore: Kysely doesn't work great with generics
      .values({
        ...updatedRows[0],
        ...values,
        valid_from: this.commitId,
        valid_until: null,
      })
      .execute();
  }

  async delete<TableName extends keyof VersionControlledTables & string>(
    tableName: TableName,
    id: string,
  ) {
    const updatedRows = await this.tx
      .updateTable(tableName)
      // @ts-ignore: Kysely doesn't work great with generics
      .where("id", "=", id)
      // @ts-ignore: Kysely doesn't work great with generics
      .where("branch_id", "=", this.branchId)
      .where("valid_until", "is", null)
      // @ts-ignore: Kysely doesn't work great with generics
      .set({ valid_until: this.commitId })
      .returningAll()
      .execute();
    assert(updatedRows.length === 1, "Expected to update exactly one row");
  }
}
