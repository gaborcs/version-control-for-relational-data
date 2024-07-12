import type { Kysely } from "kysely";
import type { VersionControlledSchema } from "./VersionControlledSchema";
import { WriteTransaction } from "./WriteTransaction";
import { createSelectAsOf } from "./createSelectAsOf";
import { createSelectLatest } from "./createSelectLatest";

export class VersionControlledDb<
  BranchMetadata,
  CommitMetadata,
  VersionControlledTables,
> {
  constructor(
    private readonly db: Kysely<
      VersionControlledSchema<
        BranchMetadata,
        CommitMetadata,
        VersionControlledTables
      >
    >,
  ) {}

  async createBranch(branchMetadata: BranchMetadata) {
    const result = await this.db
      .insertInto("branch")
      // @ts-ignore: Kysely doesn't work great with generics
      .values(branchMetadata)
      .returning("id")
      .executeTakeFirstOrThrow();
    const { id } = result as { id: number };
    return id;
  }

  async getBranchMetadata(branchId: number) {
    return (
      this.db
        .selectFrom("branch")
        // @ts-ignore: Kysely doesn't work great with generics
        .where("id", "=", branchId)
        .selectAll()
        .executeTakeFirst()
    );
  }

  executeWriteTransaction<T>(
    fn: (
      tx: WriteTransaction<
        BranchMetadata,
        CommitMetadata,
        VersionControlledTables
      >,
    ) => Promise<T>,
  ) {
    return this.db
      .transaction()
      .setIsolationLevel("serializable")
      .execute((tx) => fn(new WriteTransaction(tx)));
  }

  get selectAsOf() {
    return createSelectAsOf(this.db);
  }

  get selectLatest() {
    return createSelectLatest(this.db);
  }
}
