#ifndef JOBSCHED_FAILURE_EXPLAINER_H
#define JOBSCHED_FAILURE_EXPLAINER_H

#include <string>

#include "util.h"

namespace jobsched
{

// Asks a language model why a job failed.
// Talks to any OpenAI-compatible chat API (local Ollama by default) over HTTP
// using libcurl. Settings come from environment variables:
//   JOBSCHED_LLM_URL      default http://localhost:11434/v1/chat/completions
//   JOBSCHED_LLM_MODEL    default llama3.2
//   JOBSCHED_LLM_API_KEY  default empty (not needed for Ollama)
class FailureExplainer
{
public:
    FailureExplainer();
    ~FailureExplainer();

    // Returns false if the model could not be asked; 'error' says why.
    bool explain(const JobRecord& record, std::string& diagnosis, std::string& error) const;

    const std::string& model() const;

private:
    std::string m_url;
    std::string m_model;
    std::string m_apiKey;
};

} // namespace jobsched

#endif // JOBSCHED_FAILURE_EXPLAINER_H