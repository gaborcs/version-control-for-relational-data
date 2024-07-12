import type { Kysely } from "kysely";
import type { VersionControlledSchema } from "./VersionControlledSchema";

export function createSelectAsOf<
  BranchMetadata,
  CommitMetadata,
  VersionControlledTables,
>(
  db: Kysely<
    VersionControlledSchema<
      BranchMetadata,
      CommitMetadata,
      VersionControlledTables
    >
  >,
) {
  return function selectAsOf<
    TableName extends keyof VersionControlledTables & string,
  >(options: {
    tableName: TableName;
    branchId: number;
    commitId: number;
  }) {
    return db.selectFrom(options.tableName).where(({ eb, and, or }) =>
      and([
        // @ts-ignore: Kysely doesn't work great with generics
        eb("branch_id", "=", options.branchId),
        // @ts-ignore: Kysely doesn't work great with generics
        eb("valid_from", "<=", options.commitId),
        or([
          // @ts-ignore: Kysely doesn't work great with generics
          eb("valid_until", ">", options.commitId),
          eb("valid_until", "is", null),
        ]),
      ]),
    );
  };
}
