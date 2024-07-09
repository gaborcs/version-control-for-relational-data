import type { Transaction } from "kysely";
import { Commit } from "./Commit";
import type { VersionControlledSchema } from "./VersionControlledSchema";

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
}
