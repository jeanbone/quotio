//
//  KiroProxyManager.swift
//  Quotio - CLIProxyAPI GUI Wrapper
//
//  Sidecar process manager for kiro-proxy.
//
//  kiro-proxy is an Anthropic-compatible HTTP server that bridges to AWS Q Developer
//  (AmazonCodeWhispererStreamingService.GenerateAssistantResponse).  It exposes Kiro
//  models with a configurable prefix (default "kiro-") so Quotio's ProxyBridge can
//  route requests with matching model names to this sidecar while delegating all
//  other traffic to the primary CLIProxyAPI(Plus) binary.
//
//  Why this sidecar exists:
//    CLIProxyAPIPlus uses AmazonCodeWhispererService.SendMessageStreaming, which is
//    rejected by AWS for Q Developer enterprise subscriptions with
//    "Your subscription does not support this application".  The correct service is
//    AmazonCodeWhispererStreamingService.GenerateAssistantResponse (the same one
//    kiro-cli-chat uses), implemented here.
//

import Foundation

@MainActor
@Observable
final class KiroProxyManager {
    // MARK: - Public state

    private(set) var running: Bool = false
    private(set) var port: UInt16 = 0
    private(set) var lastError: String?

    /// Base URL callers can use to hit this proxy directly.  Returns nil when not running.
    var baseURL: String? {
        guard running, port != 0 else { return nil }
        return "http://127.0.0.1:\(port)"
    }

    // MARK: - Private state

    private var process: Process?
    private var expectedTerminationPIDs = Set<Int32>()

    // MARK: - Constants

    private static let binaryName = "kiro-proxy"
    private static let resourceSubdirectory = "Proxy"
    private static let modelPrefix = "kiro-"

    // MARK: - Lifecycle

    /// Start kiro-proxy on the requested port.  When the port is already taken by
    /// a previous instance the helper tries to terminate it before re-launching.
    /// - Parameters:
    ///   - port: TCP port kiro-proxy should bind to (loopback only).
    ///   - authFile: Optional path to a Kiro auth JSON.  When nil, kiro-proxy
    ///               discovers one in `~/.cli-proxy-api/kiro-*.json` automatically.
    func start(port: UInt16, authFile: String? = nil) async throws {
        if running {
            NSLog("[KiroProxyManager] already running on port \(self.port)")
            return
        }

        guard let binaryPath = resolveBundledBinaryPath() else {
            lastError = "kiro-proxy binary not found in app bundle"
            throw KiroProxyError.binaryMissing
        }

        // Remove any lingering listeners on that port so we can bind cleanly.
        Self.killProcessOnPort(port)

        let process = Process()
        process.executableURL = URL(fileURLWithPath: binaryPath)
        var args: [String] = [
            "--port", String(port),
            "--model-prefix", Self.modelPrefix,
        ]
        if let authFile, !authFile.isEmpty {
            args.append(contentsOf: ["--auth-file", authFile])
        }
        process.arguments = args
        process.currentDirectoryURL = URL(fileURLWithPath: binaryPath).deletingLastPathComponent()

        // Drain stdout/stderr to prevent pipe buffer deadlock.
        let outputPipe = Pipe()
        let errorPipe = Pipe()
        process.standardOutput = outputPipe
        process.standardError = errorPipe

        outputPipe.fileHandleForReading.readabilityHandler = { handle in
            _ = handle.availableData.count
        }
        errorPipe.fileHandleForReading.readabilityHandler = { handle in
            let data = handle.availableData
            if !data.isEmpty, let s = String(data: data, encoding: .utf8), !s.isEmpty {
                // kiro-proxy uses stderr for its normal log output (like many Go programs).
                NSLog("[kiro-proxy] %@", s.trimmingCharacters(in: .whitespacesAndNewlines))
            }
        }

        var environment = ProcessInfo.processInfo.environment
        environment["TERM"] = "xterm-256color"
        process.environment = environment

        process.terminationHandler = { [weak self] terminatedProcess in
            let status = terminatedProcess.terminationStatus
            let pid = terminatedProcess.processIdentifier

            outputPipe.fileHandleForReading.readabilityHandler = nil
            errorPipe.fileHandleForReading.readabilityHandler = nil
            try? outputPipe.fileHandleForReading.close()
            try? errorPipe.fileHandleForReading.close()

            Task { @MainActor in
                guard let self else { return }
                let wasExpected = self.expectedTerminationPIDs.remove(pid) != nil
                guard self.process?.processIdentifier == pid else { return }
                self.running = false
                self.process = nil
                if !wasExpected {
                    self.lastError = "kiro-proxy exited with code: \(status)"
                    NSLog("[KiroProxyManager] unexpected exit code=\(status)")
                }
            }
        }

        do {
            try process.run()
            self.process = process
            self.port = port

            // Give it a moment to bind.
            try await Task.sleep(nanoseconds: 700_000_000)

            guard process.isRunning else {
                throw KiroProxyError.startupFailed
            }

            // Smoke test: /health must respond 200 within 2 seconds
            if !(await probeHealth(port: port, timeout: 2.0)) {
                process.terminate()
                throw KiroProxyError.healthCheckFailed
            }

            self.running = true
            self.lastError = nil
            NSLog("[KiroProxyManager] started on port \(port) (pid=\(process.processIdentifier))")
        } catch {
            lastError = error.localizedDescription
            self.process = nil
            throw error
        }
    }

    func stop() {
        guard let process, process.isRunning else {
            self.process = nil
            self.running = false
            return
        }
        expectedTerminationPIDs.insert(process.processIdentifier)
        process.terminate()
        self.process = nil
        self.running = false
        NSLog("[KiroProxyManager] stopped")
    }

    // MARK: - Health check

    private func probeHealth(port: UInt16, timeout: TimeInterval) async -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await pingHealthEndpoint(port: port) { return true }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }
        return false
    }

    private nonisolated func pingHealthEndpoint(port: UInt16) async -> Bool {
        guard let url = URL(string: "http://127.0.0.1:\(port)/health") else { return false }
        var request = URLRequest(url: url)
        request.timeoutInterval = 1.0
        let config = URLSessionConfiguration.ephemeral
        config.timeoutIntervalForRequest = 1.0
        config.connectionProxyDictionary = [:]
        let session = URLSession(configuration: config)
        do {
            let (_, response) = try await session.data(for: request)
            return (response as? HTTPURLResponse)?.statusCode == 200
        } catch {
            return false
        }
    }

    // MARK: - Binary resolution

    private func resolveBundledBinaryPath() -> String? {
        let fileManager = FileManager.default
        let name = Self.binaryName
        let sub = Self.resourceSubdirectory

        let candidates: [URL?] = [
            Bundle.main.url(forResource: name, withExtension: nil, subdirectory: sub),
            Bundle.main.resourceURL?
                .appendingPathComponent(sub, isDirectory: true)
                .appendingPathComponent(name, isDirectory: false),
            Bundle.main.url(forResource: name, withExtension: nil),
            Bundle.main.resourceURL?
                .appendingPathComponent(name, isDirectory: false),
        ]
        for candidate in candidates.compactMap({ $0 }) {
            guard fileManager.fileExists(atPath: candidate.path) else { continue }
            let values = try? candidate.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey])
            if values?.isRegularFile == true, values?.isSymbolicLink != true {
                return candidate.path
            }
        }
        return nil
    }

    // MARK: - Port cleanup helper (copy of CLIProxyManager's implementation)

    nonisolated private static func killProcessOnPort(_ port: UInt16) {
        let lsofProcess = Process()
        lsofProcess.executableURL = URL(fileURLWithPath: "/usr/sbin/lsof")
        lsofProcess.arguments = ["-ti", "tcp:\(port)"]

        let pipe = Pipe()
        lsofProcess.standardOutput = pipe
        lsofProcess.standardError = FileHandle.nullDevice

        let ownPid = ProcessInfo.processInfo.processIdentifier

        do {
            try lsofProcess.run()
            lsofProcess.waitUntilExit()
            let data = pipe.fileHandleForReading.readDataToEndOfFile()
            guard let pidListing = String(data: data, encoding: .utf8) else { return }
            let pids = pidListing
                .split(separator: "\n")
                .compactMap { Int32($0.trimmingCharacters(in: .whitespaces)) }
                .filter { $0 != ownPid }
            for pid in pids {
                kill(pid, SIGTERM)
            }
        } catch {
            // best-effort cleanup, ignore
        }
    }
}

enum KiroProxyError: LocalizedError {
    case binaryMissing
    case startupFailed
    case healthCheckFailed

    var errorDescription: String? {
        switch self {
        case .binaryMissing:     return "kiro-proxy binary not found in app bundle"
        case .startupFailed:     return "kiro-proxy failed to start"
        case .healthCheckFailed: return "kiro-proxy did not respond to /health in time"
        }
    }
}
