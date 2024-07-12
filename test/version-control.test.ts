import Database from "better-sqlite3";
import { Kysely, SqliteDialect } from "kysely";
import { beforeEach, describe, expect, it } from "vitest";
import { VersionControlledDb } from "../src/VersionControlledDb";
import type { VersionControlledSchema } from "../src/VersionControlledSchema";

interface BranchMetadata {
  name: string;
}
interface CommitMetadata {
  author: string;
}
interface VersionControlledTables {
  variable: {
    name: string;
  };
}
let kysely: Kysely<
  VersionControlledSchema<
    BranchMetadata,
    CommitMetadata,
    VersionControlledTables
  >
>;
let db: VersionControlledDb<
  BranchMetadata,
  CommitMetadata,
  VersionControlledTables
>;

beforeEach(async () => {
  const sqlite = new Database(":memory:");
  kysely = new Kysely({
    dialect: new SqliteDialect({
      database: sqlite,
    }),
  });
  await kysely.schema
    .createTable("branch")
    .addColumn("id", "integer", (col) => col.primaryKey().notNull())
    .addColumn("name", "text", (col) => col.notNull())
    .execute();
  await kysely.schema
    .createTable("commit")
    .addColumn("id", "integer", (col) =>
      col.autoIncrement().primaryKey().notNull(),
    )
    .addColumn("author", "text", (col) => col.notNull())
    .execute();
  await kysely.schema
    .createTable("variable")
    .addColumn("id", "text", (col) => col.notNull())
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("branch_id", "integer", (col) => col.notNull())
    .addColumn("valid_from", "integer", (col) => col.notNull())
    .addColumn("valid_until", "integer")
    .execute();
  db = new VersionControlledDb(kysely);
  return () => {
    sqlite.close();
  };
});

describe("VersionControlledDb", () => {
  describe("createBranch", () => {
    it("should save the metadata of the created branch", async () => {
      await db.createBranch({ name: "main" });

      const branch = await kysely
        .selectFrom("branch")
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(branch.name).toBe("main");
    });

    it("should return the ID of the created branch", async () => {
      const returnedId = await db.createBranch({ name: "main" });

      const branch = await kysely
        .selectFrom("branch")
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(returnedId).toEqual(branch.id);
    });
  });

  describe("getBranchMetadata", () => {
    it("should return the metadata when the branch exists", async () => {
      const branchId = await db.createBranch({ name: "main" });

      const branchMetadata = await db.getBranchMetadata(branchId);

      expect(branchMetadata).toEqual({ id: branchId, name: "main" });
    });

    it("should return undefined when the branch doesn't exist", async () => {
      const branchMetadata = await db.getBranchMetadata(1);

      expect(branchMetadata).toBeUndefined();
    });
  });

  describe("executeWriteTransaction", () => {
    it("should return whatever the passed function returns", async () => {
      const returnValue = await db.executeWriteTransaction(async () => {
        return 42;
      });

      expect(returnValue).toBe(42);
    });
  });

  testSelectAsOf(function executeSelectAsOf(options) {
    return db.selectAsOf(options).selectAll().execute();
  });

  testSelectLatest(function executeSelectLatest(options) {
    return db.selectLatest(options).selectAll().execute();
  });
});

describe("WriteTransaction", () => {
  describe("createCommit", () => {
    it("should save the metadata of the created commit", async () => {
      const branchId = await db.createBranch({ name: "main" });

      await db.executeWriteTransaction(async (tx) => {
        await tx.createCommit(branchId, { author: "Alice" });
      });

      const commit = await kysely
        .selectFrom("commit")
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(commit.author).toBe("Alice");
    });
  });

  testSelectAsOf(function executeSelectAsOf(options) {
    return db.executeWriteTransaction(async (tx) => {
      return tx.selectAsOf(options).selectAll().execute();
    });
  });

  testSelectLatest(function executeSelectLatest(options) {
    return db.executeWriteTransaction(async (tx) => {
      return tx.selectLatest(options).selectAll().execute();
    });
  });
});

describe("Commit", () => {
  describe("insert", () => {
    it("should store inserted rows as valid from the current commit on the given branch", async () => {
      const branchId = await db.createBranch({ name: "main" });

      const commitId = await db.executeWriteTransaction(async (tx) => {
        const commit = await tx.createCommit(branchId, { author: "Alice" });
        await commit.insert("variable", { name: "Insulin resistance" });
        return commit.id;
      });

      expect(commitId).toEqual(expect.any(Number));
      const variable = await kysely
        .selectFrom("variable")
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(variable).toMatchObject({
        name: "Insulin resistance",
        branch_id: branchId,
        valid_from: commitId,
        valid_until: null,
      });
    });

    it("should return the ID of the inserted row", async () => {
      const branchId = await db.createBranch({ name: "main" });

      const returnedId = await db.executeWriteTransaction(async (tx) => {
        const commit = await tx.createCommit(branchId, { author: "Alice" });
        return commit.insert("variable", { name: "Insulin resistance" });
      });

      const variable = await kysely
        .selectFrom("variable")
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(returnedId).toEqual(expect.any(String));
      expect(returnedId).toBe(variable.id);
    });
  });

  describe("update", () => {
    it("should mark the previous row as invalid from the current commit", async () => {
      const branchId = await db.createBranch({ name: "main" });
      const { variableId, commitId: previousCommit } =
        await db.executeWriteTransaction(async (tx) => {
          const commit = await tx.createCommit(branchId, { author: "Alice" });
          const variableId = await commit.insert("variable", {
            name: "Insulin resistance",
          });
          return { variableId, commitId: commit.id };
        });

      const commitId = await db.executeWriteTransaction(async (tx) => {
        const commit = await tx.createCommit(branchId, { author: "Alice" });
        await commit.update("variable", variableId, {
          name: "Insulin sensitivity",
        });
        return commit.id;
      });

      const variable = await kysely
        .selectFrom("variable")
        .where("valid_until", "=", commitId)
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(variable).toEqual({
        id: variableId,
        name: "Insulin resistance",
        branch_id: branchId,
        valid_from: previousCommit,
        valid_until: commitId,
      });
    });

    it("should store the updated row as valid from the current commit on the given branch", async () => {
      const branchId = await db.createBranch({ name: "main" });
      const variableId = await db.executeWriteTransaction(async (tx) => {
        const commit = await tx.createCommit(branchId, { author: "Alice" });
        return await commit.insert("variable", { name: "Insulin resistance" });
      });

      const commitId = await db.executeWriteTransaction(async (tx) => {
        const commit = await tx.createCommit(branchId, { author: "Alice" });
        await commit.update("variable", variableId, {
          name: "Insulin sensitivity",
        });
        return commit.id;
      });

      const variable = await kysely
        .selectFrom("variable")
        .where("valid_from", "=", commitId)
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(variable).toEqual({
        id: variableId,
        name: "Insulin sensitivity",
        branch_id: branchId,
        valid_from: commitId,
        valid_until: null,
      });
    });
  });

  describe("delete", () => {
    it("should mark the row as invalid from the current commit", async () => {
      const branchId = await db.createBranch({ name: "main" });
      const { variableId, commitId: previousCommit } =
        await db.executeWriteTransaction(async (tx) => {
          const commit = await tx.createCommit(branchId, { author: "Alice" });
          const variableId = await commit.insert("variable", {
            name: "Insulin resistance",
          });
          return { variableId, commitId: commit.id };
        });

      const commitId = await db.executeWriteTransaction(async (tx) => {
        const commit = await tx.createCommit(branchId, { author: "Alice" });
        await commit.delete("variable", variableId);
        return commit.id;
      });

      const variable = await kysely
        .selectFrom("variable")
        .where("valid_until", "=", commitId)
        .selectAll()
        .executeTakeFirstOrThrow();
      expect(variable).toEqual({
        id: variableId,
        name: "Insulin resistance",
        branch_id: branchId,
        valid_from: previousCommit,
        valid_until: commitId,
      });
    });
  });
});

function testSelectAsOf(
  executeSelectAsOf: <
    TableName extends keyof VersionControlledTables,
  >(options: {
    tableName: TableName;
    branchId: number;
    commitId: number;
  }) => Promise<unknown>,
) {
  describe("selectAsOf", () => {
    it("should return rows that were introduced in the specified commit", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 1,
          },
          {
            id: "2",
            name: "Diabetes",
            branch_id: 1,
            valid_from: 1,
            valid_until: 3,
          },
        ])
        .execute();

      const variables = await executeSelectAsOf({
        tableName: "variable",
        branchId: 1,
        commitId: 1,
      });

      expect(variables).toMatchObject([{ id: "1" }, { id: "2" }]);
    });

    it("should return rows that were introduced before the specified commit and not invalidated by the specified commit", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 1,
          },
          {
            id: "2",
            name: "Diabetes",
            branch_id: 1,
            valid_from: 1,
            valid_until: 3,
          },
        ])
        .execute();

      const variables = await executeSelectAsOf({
        tableName: "variable",
        branchId: 1,
        commitId: 2,
      });

      expect(variables).toMatchObject([{ id: "1" }, { id: "2" }]);
    });

    it("should not return rows that were introduced on a different branch", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 1,
          },
        ])
        .execute();

      const variables = await executeSelectAsOf({
        tableName: "variable",
        branchId: 2,
        commitId: 1,
      });

      expect(variables).toMatchObject([]);
    });

    it("should not return rows that were introduced after the specified commit", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 2,
          },
          {
            id: "2",
            name: "Diabetes",
            branch_id: 1,
            valid_from: 2,
            valid_until: 3,
          },
        ])
        .execute();

      const variables = await executeSelectAsOf({
        tableName: "variable",
        branchId: 1,
        commitId: 1,
      });

      expect(variables).toMatchObject([]);
    });

    it("should not return rows that were removed by the specified commit", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 1,
            valid_until: 2,
          },
          {
            id: "2",
            name: "Diabetes",
            branch_id: 1,
            valid_from: 1,
            valid_until: 3,
          },
        ])
        .execute();

      const variables = await executeSelectAsOf({
        tableName: "variable",
        branchId: 1,
        commitId: 3,
      });

      expect(variables).toMatchObject([]);
    });
  });
}

function testSelectLatest(
  executeSelectLatest: <
    TableName extends keyof VersionControlledTables,
  >(options: { tableName: TableName; branchId: number }) => Promise<unknown>,
) {
  describe("selectLatest", () => {
    it("should return rows that are still valid", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 1,
          },
        ])
        .execute();

      const variables = await executeSelectLatest({
        tableName: "variable",
        branchId: 1,
      });

      expect(variables).toMatchObject([{ id: "1" }]);
    });

    it("should not return rows that are not valid anymore", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 1,
            valid_until: 2,
          },
        ])
        .execute();

      const variables = await executeSelectLatest({
        tableName: "variable",
        branchId: 1,
      });

      expect(variables).toMatchObject([]);
    });

    it("should not return rows that are valid on a different branch", async () => {
      await kysely
        .insertInto("variable")
        .values([
          {
            id: "1",
            name: "Obesity",
            branch_id: 1,
            valid_from: 1,
          },
        ])
        .execute();

      const variables = await executeSelectLatest({
        tableName: "variable",
        branchId: 2,
      });

      expect(variables).toMatchObject([]);
    });
  });
}
