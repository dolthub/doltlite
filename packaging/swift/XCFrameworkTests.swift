import Foundation
import XCTest
import Doltlite

final class XCFrameworkTests: XCTestCase {
    func testFileBackedCommitSurvivesReopen() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("smoke.db").path
        let message = "XCFramework consumer commit"
        let hash: String

        do {
            let db = try Doltlite(path)
            XCTAssertEqual(try db.query("SELECT doltlite_engine(), dolt_version()"), [["prolly", expectedEngineVersion]])
            try db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
            try db.execute("INSERT INTO t VALUES (1, 'café'), (2, NULL)")
            try db.execute("BEGIN; UPDATE t SET value = 'rolled back'; ROLLBACK")
            hash = try XCTUnwrap(db.commit(message: message))
            XCTAssertFalse(hash.isEmpty)
        }

        let reopened = try Doltlite(path)
        XCTAssertEqual(try reopened.query("SELECT id, value FROM t ORDER BY id"), [["1", "café"], ["2", nil]])
        XCTAssertEqual(try reopened.query("SELECT commit_hash FROM dolt_log WHERE message = '\(message)'"), [[hash]])
    }
}
