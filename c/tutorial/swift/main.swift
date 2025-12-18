import Foundation
import Darwin

// Entry point for the tutorial binary.
// The reusable wrappers live in FlintDBSwift.swift and tutorial flow in tutorial.swift.

do {
	try runTutorial()
	print("All tutorial steps completed successfully.")
	
	// Cleanup resources
	flintCleanup()
} catch {
	fputs("\(error)\n", stderr)
	flintCleanup()
	exit(1)
}
