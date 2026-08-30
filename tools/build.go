//go:build tools

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	imageTag := flag.String("tag", "ingest:latest", "Image tag to build")
	platforms := flag.String("platforms", "linux/amd64,linux/arm64", "Comma-separated target platforms")
	pushImage := flag.Bool("push", true, "Push the image manifest to a registry")
	dockerfilePath := flag.String("dockerfile", "Dockerfile", "Path to the Dockerfile")
	contextPath := flag.String("context", ".", "Build context directory")
	flag.Parse()

	normalizedPlatforms := normalizePlatforms(*platforms)
	if len(normalizedPlatforms) == 0 {
		fmt.Fprintln(os.Stderr, "at least one platform must be provided")
		os.Exit(1)
	}

	if len(normalizedPlatforms) > 1 && !*pushImage {
		fmt.Fprintln(os.Stderr, "multi-arch builds require -push=true because docker buildx cannot --load multiple platforms")
		os.Exit(1)
	}

	args := []string{"buildx", "build", "--platform", strings.Join(normalizedPlatforms, ","), "-t", *imageTag, "-f", *dockerfilePath}
	if *pushImage {
		args = append(args, "--push")
	} else {
		args = append(args, "--load")
	}
	args = append(args, *contextPath)

	fmt.Printf("Running: docker %s\n", strings.Join(args, " "))

	cmd := exec.Command("docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "docker buildx build failed:", err)
		os.Exit(1)
	}
}

func normalizePlatforms(value string) []string {
	parts := strings.Split(value, ",")
	platforms := make([]string, 0, len(parts))

	for _, part := range parts {
		platform := strings.TrimSpace(part)
		if platform != "" {
			platforms = append(platforms, platform)
		}
	}

	return platforms
}