
bakusa.py の実装に当たりまとめるもの

antena 上では新規カテゴリ(SuperCategory にbakusai)を配置し、店舗の掲示板か、個人による掲示板でサブカテゴリを2つに分ける

background 側で抽出する構造体は以下の２つを基本とする

```
スレッドのそれぞれの概要

ThreadInfo {
  # 加工されていない、表示されている生のスレッドの名前
  thread_name: str
  # thread_name から加工し、取得できた店舗の名前
  shop_name: str
  # thread_name から加工し、取得できたスレッド番号
  shop_thread_number: int
  # そのスレッドの絶対パスのURL
  link: str
  # そのスレッドにおいて最後にコメントをしたのがいつであるのか : format(YYYY/MM/DD HH:MM)
  last_commented: datetime
  # bakusai 上で表示されている現在の閲覧数
  viewer: int
  # スレッド上で、1-1000の間でどれくらいまでレスがすすんでいるのか、最後のレスID
  res_number: int
  # そのスレッド上でのそれぞれのresの情報
  comments: List[ResInfo];
}

スレッド内のそれぞれのレスについての情報

ResInfo {
  # レスID,1-1000の間の範囲をとる
  res_id: int;
  # そのレスが特定のレスに対する返信であるかどうか
  replay_to_id: Optional[int];
  # いつそのレスを打ったのか format(YYYY/MM/DD HH:MM);
  comment_time: datetime;
  # レスの内容、文字列
  comment_text: str;
  # 誰がそのレスを打ったのか、基本的には匿名が多い
  typed_name: string;

}

```

これは python 上で class として表現をし、必要な情報が取得できるよう関数ベースではなく、class method ベースで追加をする

そのため、処理面でみるとこのbakusai.py は DBに保存するために生の情報を加工してほしい情報だけを抽出するロジック、そして抽出後それをDBに保存する２つのロジックに分けられる。

bakusai.py で行うのはあくまでDBに保存するための生の情報を加工し必要な構造化されてデータに格納することである。

db に対してのCRUD操作を行う場合には repositories.py にすべて集約する。

現在repositories.py には Supabase の AsyncClient インスタンスを管理するシングルトンがあるが、これのほかにもクラスを追加する。

repositories.py には ARTICLE_TABLE, SITE_TABLE, BOOKMARK_TABLE, ALLOW_HOST table,  GENERAL_REMOVE_TAGS table, supabase 上に テーブルではないが、RPCとして登録されている関数がある。

それらのテーブルごとに対して以下のようなクラスを作成し、よりいいコードをにしなさい。
以下のコードは Bakusai の情報を操作するクラス

```
 class BakusaiRepository:
     def __init__(self, supabase_client):
         self.db = supabase_client

     async def get_thread_by_link(self, thread_link: str) -> Optional[Dict]:
         # find_thread_by_link のロジックをここに移動
         ...

     async def create_thread(self, thread: ThreadInfo) -> Optional[int]:
         # save_thread_to_db のロジックをここに移動
         ...

     # 他のDB操作メソッドも同様に...

```

これらの内容を踏まえて、可読性と保守性、堅牢性、テスト容易性を担保できるコードにしなさい。

コードを修正した後には、uv run ruff check . && uv run pyright でエラーがないのかを確認せよ。
エラーが表示される場合にはその出力内容を確認し修正せよ
