use crate::client::{Client, RequestOptions};
use crate::error::ApiResult;
use crate::models::explorer::Share;
use crate::models::share::{
    BatchDeleteShareService, ListShareResponse, ListShareService, ShareCreateService,
};
use async_trait::async_trait;

/// Share-link CRUD for the Cloudreve v4 API.
#[async_trait]
pub trait ShareApi {
    /// Create a share link and return its URL.
    async fn create_share(&self, request: &ShareCreateService) -> ApiResult<String>;

    /// Update an existing share link and return its URL.
    async fn update_share(&self, id: &str, request: &ShareCreateService) -> ApiResult<String>;

    /// Get share metadata. `password` is required for protected links when the
    /// caller is not the owner.
    async fn get_share(
        &self,
        id: &str,
        password: Option<&str>,
        count_views: bool,
        owner_extended: bool,
    ) -> ApiResult<Share>;

    /// Like [`ShareApi::get_share`], but with explicit request options. Use
    /// `RequestOptions::no_credential()` for anonymous visitors.
    async fn get_share_with_options(
        &self,
        id: &str,
        password: Option<&str>,
        count_views: bool,
        owner_extended: bool,
        options: RequestOptions,
    ) -> ApiResult<Share>;

    /// List the current user's share links.
    async fn list_shares(&self, request: &ListShareService) -> ApiResult<ListShareResponse>;

    /// Delete one share link.
    async fn delete_share(&self, id: &str) -> ApiResult<()>;

    /// Delete multiple share links in one request.
    async fn delete_shares(&self, ids: &[String]) -> ApiResult<()>;
}

#[async_trait]
impl ShareApi for Client {
    async fn create_share(&self, request: &ShareCreateService) -> ApiResult<String> {
        self.put("/share", request, RequestOptions::new()).await
    }

    async fn update_share(&self, id: &str, request: &ShareCreateService) -> ApiResult<String> {
        self.post(
            &format!("/share/{}", urlencoding::encode(id)),
            request,
            RequestOptions::new(),
        )
        .await
    }

    async fn get_share(
        &self,
        id: &str,
        password: Option<&str>,
        count_views: bool,
        owner_extended: bool,
    ) -> ApiResult<Share> {
        self.get_share_with_options(
            id,
            password,
            count_views,
            owner_extended,
            RequestOptions::new(),
        )
        .await
    }

    async fn get_share_with_options(
        &self,
        id: &str,
        password: Option<&str>,
        count_views: bool,
        owner_extended: bool,
        options: RequestOptions,
    ) -> ApiResult<Share> {
        let mut query = Vec::new();
        if let Some(password) = password.filter(|value| !value.is_empty()) {
            query.push(format!("password={}", urlencoding::encode(password)));
        }
        query.push(format!("count_views={count_views}"));
        query.push(format!("owner_extended={owner_extended}"));
        self.get(
            &format!(
                "/share/info/{}?{}",
                urlencoding::encode(id),
                query.join("&")
            ),
            options,
        )
        .await
    }

    async fn list_shares(&self, request: &ListShareService) -> ApiResult<ListShareResponse> {
        let mut query = vec![format!("page_size={}", request.page_size.clamp(10, 100))];
        if let Some(order_by) = &request.order_by {
            query.push(format!("order_by={}", urlencoding::encode(order_by)));
        }
        if let Some(direction) = &request.order_direction {
            query.push(format!(
                "order_direction={}",
                urlencoding::encode(direction)
            ));
        }
        if let Some(token) = &request.next_page_token {
            query.push(format!("next_page_token={}", urlencoding::encode(token)));
        }
        self.get(
            &format!("/share?{}", query.join("&")),
            RequestOptions::new(),
        )
        .await
    }

    async fn delete_share(&self, id: &str) -> ApiResult<()> {
        let _: serde_json::Value = self
            .delete(
                &format!("/share/{}", urlencoding::encode(id)),
                RequestOptions::new(),
            )
            .await?;
        Ok(())
    }

    async fn delete_shares(&self, ids: &[String]) -> ApiResult<()> {
        let _: serde_json::Value = self
            .delete_with_body(
                "/share",
                &BatchDeleteShareService {
                    ids: ids.to_vec(),
                },
                RequestOptions::new(),
            )
            .await?;
        Ok(())
    }
}
