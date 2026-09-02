use std::path::Path;

use anyhow::{Context, Result, anyhow};
use candle_core::{D, DType, Device, Module, Tensor};
use candle_nn::{Linear, VarBuilder, linear};
use candle_transformers::models::bert::{BertModel, Config};
use tokenizers::{Encoding, PaddingParams, Tokenizer, TruncationParams, TruncationStrategy};

const MAX_TOKENS: usize = 256;
const BATCH_SIZE: usize = 16;

pub struct Reranker {
    bert: BertModel,
    pooler: Linear,
    classifier: Linear,
    tokenizer: Tokenizer,
}

impl Reranker {
    pub fn load_dir(dir: &Path) -> Result<Self> {
        let read = |name: &str| {
            std::fs::read(dir.join(name))
                .with_context(|| format!("failed to read {}", dir.join(name).display()))
        };
        let config = String::from_utf8(read("config.json")?)?;
        Self::from_parts(
            &config,
            read("model.safetensors")?,
            &read("tokenizer.json")?,
        )
    }

    pub fn from_parts(
        config_json: &str,
        safetensors: Vec<u8>,
        tokenizer_json: &[u8],
    ) -> Result<Self> {
        let config: Config =
            serde_json::from_str(config_json).context("invalid BERT config.json")?;
        let vb = VarBuilder::from_buffered_safetensors(safetensors, DType::F32, &Device::Cpu)?;
        let prefix = config
            .model_type
            .clone()
            .unwrap_or_else(|| "bert".to_string());
        let hidden = config.hidden_size;
        let bert = BertModel::load(vb.clone(), &config).context("failed to load BERT weights")?;
        let pooler = linear(hidden, hidden, vb.pp(format!("{prefix}.pooler.dense")))?;
        let classifier = linear(hidden, 1, vb.pp("classifier"))?;
        let mut tokenizer = Tokenizer::from_bytes(tokenizer_json).map_err(|e| anyhow!("{e}"))?;
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: MAX_TOKENS,
                strategy: TruncationStrategy::OnlySecond,
                ..Default::default()
            }))
            .map_err(|e| anyhow!("{e}"))?;
        tokenizer.with_padding(Some(PaddingParams::default()));
        Ok(Self {
            bert,
            pooler,
            classifier,
            tokenizer,
        })
    }

    pub fn score(&self, query: &str, passages: &[String]) -> Result<Vec<f32>> {
        let mut scores = Vec::with_capacity(passages.len());
        for chunk in passages.chunks(BATCH_SIZE) {
            let pairs: Vec<(&str, &str)> = chunk.iter().map(|p| (query, p.as_str())).collect();
            let encodings = self
                .tokenizer
                .encode_batch(pairs, true)
                .map_err(|e| anyhow!("{e}"))?;
            let ids = tensor(&encodings, Encoding::get_ids)?;
            let types = tensor(&encodings, Encoding::get_type_ids)?;
            let mask = tensor(&encodings, Encoding::get_attention_mask)?;
            let hidden = self.bert.forward(&ids, &types, Some(&mask))?;
            let cls = hidden.narrow(1, 0, 1)?.squeeze(1)?;
            let pooled = self.pooler.forward(&cls)?.tanh()?;
            let logits = self.classifier.forward(&pooled)?.squeeze(D::Minus1)?;
            scores.extend(logits.to_vec1::<f32>()?);
        }
        Ok(scores)
    }
}

fn tensor(encodings: &[Encoding], field: fn(&Encoding) -> &[u32]) -> Result<Tensor> {
    let width = encodings.first().map_or(0, Encoding::len);
    let flat: Vec<u32> = encodings
        .iter()
        .flat_map(|e| field(e).iter().copied())
        .collect();
    Ok(Tensor::from_vec(
        flat,
        (encodings.len(), width),
        &Device::Cpu,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "needs a downloaded model in ORBIT_RERANK_MODEL_DIR"]
    fn matches_sentence_transformers_reference_scores() {
        let dir = std::env::var("ORBIT_RERANK_MODEL_DIR").expect("model dir");
        let reranker = Reranker::load_dir(Path::new(&dir)).unwrap();
        let passages = [
            "Berlin has a population of 3,520,031 registered inhabitants in an area of 891.82 square kilometers.",
            "New York City is famous for the Metropolitan Museum of Art.",
            "fn ask(source, question, limit) -> AskOutcome { let terms = content_words(question); }",
        ]
        .map(String::from);
        let scores = reranker
            .score("How many people live in Berlin?", &passages)
            .unwrap();
        eprintln!("scores: {scores:?}");
        assert!(scores[0] > scores[1] && scores[0] > scores[2]);
    }
}
