use serde_json::Value;
use std::process::Command;

pub fn current_context() -> String {
    String::from_utf8(
        Command::new("kubectl")
            .arg("config")
            .arg("current-context")
            .output()
            .expect("Could not determine the current context")
            .stdout,
    )
    .expect("Unexpected error trying to convert bytes to UTF8 string")
    .trim()
    .to_owned()
}

pub fn namespaces() -> Vec<String> {
    retrieve_k8s_resources(vec!["get", "namespace", "-ojson"])
}

pub fn pods(namespace: &str) -> Vec<String> {
    retrieve_k8s_resources(vec!["get", "pods", "--namespace", namespace, "-ojson"])
}

fn retrieve_k8s_resources(kubectl_args: Vec<&str>) -> Vec<String> {
    log::debug!("Trying to retrieve k8s resources with {:?}", kubectl_args);
    // Vec to store the retrieved resource names
    let mut resources = Vec::new();
    let cmd_output = Command::new("kubectl").args(kubectl_args).output();

    if let Ok(cmd_output) = cmd_output {
        let result: Value = serde_json::from_slice(&cmd_output.stdout).unwrap_or(Value::Null);
        if !result.is_null() {
            // Option.unwrap_or requires that we use a reference because Value.get return a Option<&Value>
            // so Option.unwrap_or uses that too
            for resource_object in result
                .get("items")
                .unwrap_or(&Value::Array(vec![]))
                .as_array()
                .unwrap_or(&Vec::<Value>::new())
            {
                if let Some(resource_object) = resource_object.get("metadata") {
                    resources.push(
                        resource_object
                            .get("name")
                            .unwrap()
                            .to_string()
                            .replace('\"', ""),
                    );
                } else {
                    log::debug!(
                        "Could not get namespace metadata from {:?}",
                        resource_object
                    );
                }
            }
        } else {
            log::debug!("Could not parse kubectl output");
        }
    } else {
        log::error!(
            "Could not get kubernetes resources\nExited with {:?}",
            cmd_output
        )
    }

    resources
}
