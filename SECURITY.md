# Security policy

## Public repository rules

This is a public portfolio repository. Do not commit:

- AWS access keys, CLI credentials, SSO tokens, or Terraform state files.
- API keys, passwords, connection strings, `.env` files, or private keys.
- Real customer, employee, payment, or production business data.
- Screenshots containing account numbers, personal email, browser tabs, or
  credentials.

The `.gitignore` file blocks common local secret files, but it cannot detect
everything. Review `git status` and `git diff --cached` before every commit.

## Local configuration

Copy `.env.example` to `.env` only for local development, then replace every
placeholder with your own non-production local values. Never upload `.env`.

AWS CLI sessions should use temporary browser-based sign-in credentials where
possible. Do not create or paste long-lived access keys into this repository.

## Reporting a concern

If you discover sensitive information in the repository, do not open a public
issue containing the value. Contact the repository owner privately, revoke or
rotate the exposed credential immediately, then remove it from the repository
and its Git history if necessary.
