import type { Transaction } from "kysely";
import { Commit } from "./Commit";
import type { VersionControlledSchema } from "./VersionControlledSchema";
import { createSelectAsOf } from "./createSelectAsOf";
import { createSelectLatest } from "./createSelectLatest";

export class WriteTransaction<
  BranchMetadata,
  CommitMetadata,
  VersionControlledTables,
> {
  constructor(
    private readonly tx: Transaction<
      VersionControlledSchema<
        BranchMetadata,
        CommitMetadata,
        VersionControlledTables
      >
    >,
  ) {}

  async createCommit(branchId: number, commitMetadata: CommitMetadata) {
    const result = await this.tx
      .insertInto("commit")
      // @ts-ignore: Kysely doesn't work great with generics
      .values(commitMetadata)
      .returning("id")
      .executeTakeFirstOrThrow();
    const { id: commitId } = result as { id: number };
    return new Commit(branchId, commitId, this.tx);
  }

  get selectAsOf() {
    return createSelectAsOf(this.tx);
  }

  get selectLatest() {
    return createSelectLatest(this.tx);
  }
}
