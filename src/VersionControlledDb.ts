import type { Kysely } from "kysely";
import type { VersionControlledSchema } from "./VersionControlledSchema";
import { WriteTransaction } from "./WriteTransaction";

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

  async executeWriteTransaction<T>(
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
}
