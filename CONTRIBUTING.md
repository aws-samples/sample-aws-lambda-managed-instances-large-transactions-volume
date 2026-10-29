# Contributing Guidelines

Thank you for your interest in contributing to this project. Whether it's a bug
report, new feature, correction, or additional documentation, we value feedback
and contributions from the community.

Please read through this document before submitting any issues or pull requests
to ensure we have the information needed to effectively respond to your bug
report or contribution.

## Reporting bugs and feature requests

Use the GitHub issue tracker to report bugs or suggest features. Before filing a
new issue, please check existing open and recently closed issues to make sure the
same report has not already been submitted.

A useful bug report typically includes:

- A reproducible test case or series of steps.
- The version of the sample you are using (commit SHA is ideal).
- Any modifications you have made relative to the published code.
- Anything unusual about your environment or deployment.

## Reporting security issues

If you discover a potential security issue in this project, please follow the
reporting process in [`SECURITY.md`](./SECURITY.md). Please do **not** create a
public GitHub issue for security findings.

## Contributing via pull requests

Contributions via pull requests are welcome. Before sending a pull request,
please make sure that:

1. You are working against the latest source on the `main` branch.
2. You have checked existing open and recently merged pull requests to make sure
   the same change is not already in flight.
3. You have opened an issue to discuss any significant work. We would hate for
   your time to be wasted on a change that does not fit the direction of the
   sample.

To send us a pull request:

1. Fork the repository.
2. Make the change in your fork, scoped to the specific issue you are addressing.
   Please avoid reformatting unrelated code in the same change.
3. Verify the SAM templates still validate (`sam validate --lint` on each) and
   that the handler files parse (`python3 -c "import ast; ast.parse(open('<file>').read())"`).
4. Commit using the Conventional Commits convention (`feat:`, `fix:`, `docs:`,
   `chore:`, etc.) with clear subject and body.
5. Open the pull request and respond to any CI findings or review comments.

## Code of conduct

This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information, see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq),
or contact opensource-codeofconduct@amazon.com with any additional questions or
comments.

## Licensing

See the [LICENSE](./LICENSE) file for the project's licensing. By submitting a
contribution you agree that it will be published under the same license.
