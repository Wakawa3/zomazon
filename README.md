How To Run zomazon.com

Webブラウザの実装にPython 3.9.13を使用した

1. 
scalarDBworkディレクトリに移動。

2.
docker-compose up -d
を実行。

3.
./gradlew run --args="LoadInitialData"
を実行。

4.
python3 test.py
を実行。

5.
ブラウザでlocalhost:8765にアクセス

6.
ブラウザでは商品の検索と購入ができ、商品の価格をクリックするとリクエストを送信し、商品が購入される。在庫0の商品もクリックできるが、その場合購入したことにならず、在庫は0のまま変化しない。