import type { Kysely } from "kysely";
import type { VersionControlledSchema } from "./VersionControlledSchema";

export function createSelectLatest<
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
  return function selectLatest<
    TableName extends keyof VersionControlledTables & string,
  >(options: {
    tableName: TableName;
    branchId: number;
  }) {
    return db.selectFrom(options.tableName).where(({ eb, and }) =>
      and([
        // @ts-ignore: Kysely doesn't work great with generics
        eb("branch_id", "=", options.branchId),
        eb("valid_until", "is", null),
      ]),
    );
  };
}
