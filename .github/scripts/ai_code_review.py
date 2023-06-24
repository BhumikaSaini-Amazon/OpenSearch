import os
import openai
import sys


def create_review(pull_request_number):
    pr = get_pull_request_details(pull_request_number)

    # Extract relevant details
    pr_title = pr['title']
    pr_body = pr['body']
    pr_files = pr['files']

    # Construct the prompt for the model, based on PR details
    prompt = f"Pull Request Title: {pr_title}\n"
    prompt += f"Pull Request Description:\n {pr_body}\n\n"

    for file in pr_files:
        filename = file['filename']
        additions = file['additions']
        deletions = file['deletions']
        changes = additions + deletions
        prompt += f"File: {filename} - Changes: {changes}\n"

    # Invoke the model to generate the review
    review = generate_review(prompt)

    # Add a comment to the pull request with the generated review
    add_pull_request_comment(review, pull_request_number)


def get_pull_request_details(pull_request_number):
    response = make_github_api_request(f"pulls/{pull_request_number}")
    
    return response.json()


def generate_review(prompt):
    # Set up OpenAI API client
    openai.api_key = os.getenv('OPENAI_API_KEY')

    # Make an API call to the model to generate the review
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        temperature=0.2,
        max_tokens=200,
        n=1,
        stop=None,
    )

    # Extract the review from the API response
    review = response.choices[0].text.strip()
    
    return review


def add_pull_request_comment(comment, pull_request_number):
    response = make_github_api_request(f"issues/{pull_request_number}/comments", method="POST", data={"body": comment})
    
    # Check the response status code to ensure the comment was added successfully
    if response.status_code == 201:
        print(f"Comment added successfully to pull request #{pull_request_number}")
    else:
        print(f"Failed to add comment to pull request #{pull_request_number}")


def make_github_api_request(path, method="GET", data=None):
    response = requests.request(method, f"https://api.github.com/repos/:owner/:repo/{path}", json=data, headers={"Authorization": f"token {os.getenv('GITHUB_TOKEN')}"})

    # Check the response status code to handle errors or rate limiting
    response.raise_for_status()
    
    return response


if __name__ == "__main__":
    pull_request_number = int(sys.argv[1])
    create_review(pull_request_number)
