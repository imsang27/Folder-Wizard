import os
import shutil
import json
import datetime
import uuid
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict

class OperationLogger:
    def __init__(self):
        self.log_dir = "operation_logs"
        os.makedirs(self.log_dir, exist_ok=True)
        
    def create_operation_log(self) -> str:
        """새로운 작업 로그 생성"""
        operation_id = str(uuid.uuid4())
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_data = {
            "id": operation_id,
            "timestamp": timestamp,
            "status": "in_progress",
            "moves": []
        }
        
        with open(os.path.join(self.log_dir, f"{operation_id}.json"), 'w') as f:
            json.dump(log_data, f, indent=2)
            
        return operation_id
        
    def log_file_move(self, operation_id: str, source: str, destination: str) -> None:
        """파일 이동 기록"""
        log_file = os.path.join(self.log_dir, f"{operation_id}.json")
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                log_data = json.load(f)
            
            log_data["moves"].append({
                "source": source,
                "destination": destination,
                "timestamp": datetime.datetime.now().isoformat()
            })
            
            with open(log_file, 'w') as f:
                json.dump(log_data, f, indent=2)
        
    def get_operation_by_timestamp(self, timestamp: str) -> Optional[Dict]:
        """타임스탬프로 작업 검색"""
        for file in os.listdir(self.log_dir):
            if file.endswith('.json'):
                with open(os.path.join(self.log_dir, file), 'r') as f:
                    log_data = json.load(f)
                    if log_data["timestamp"] == timestamp:
                        return log_data
        return None
        
    def get_log_file(self, operation_id: str) -> Optional[str]:
        """작업 ID로 로그 파일 검색"""
        log_file = f"{operation_id}.json"
        if os.path.exists(os.path.join(self.log_dir, log_file)):
            return log_file
        return None
        
    def get_recent_operations(self, limit: int = 10) -> List[Dict]:
        """최근 작업 이력 조회"""
        operations = []
        for file in os.listdir(self.log_dir):
            if file.endswith('.json'):
                with open(os.path.join(self.log_dir, file), 'r') as f:
                    log_data = json.load(f)
                    operations.append(log_data)
        
        # 타임스탬프 기준 정렬
        operations.sort(key=lambda x: x["timestamp"], reverse=True)
        return operations[:limit]

    def complete_operation(self, operation_id: str) -> None:
        """작업 완료 상태 업데이트"""
        log_file = self.get_log_file(operation_id)
        if log_file:
            with open(os.path.join(self.log_dir, log_file), 'r+') as f:
                log_data = json.load(f)
                log_data["status"] = "completed"
                log_data["completed_at"] = datetime.datetime.now().isoformat()
                f.seek(0)
                json.dump(log_data, f, indent=2)
                f.truncate()

class FolderWizard:
    def __init__(self):
        self.logger = OperationLogger()
        self.current_operation_id = None
        self.current_file = None
        self.start_time = None
        self.is_paused = False
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'errors': [],
            'start_time': None,
            'end_time': None,
            'pause_time': None,
            'total_pause_duration': datetime.timedelta(),
            'success_count': 0,
            'error_types': defaultdict(int)
        }

    def update_progress(self):
        """진행 상황 업데이트"""
        if self.stats['total_files'] > 0:
            progress = (self.stats['processed_files'] / self.stats['total_files']) * 100
            current_time = datetime.datetime.now()
            
            if self.start_time:
                elapsed_time = current_time - self.start_time
                remaining_files = self.stats['total_files'] - self.stats['processed_files']
                
                # 예상 남은 시간 계산
                if self.stats['processed_files'] > 0:
                    avg_time_per_file = elapsed_time / self.stats['processed_files']
                    estimated_remaining = avg_time_per_file * remaining_files
                    remaining_time = str(estimated_remaining).split('.')[0]
                else:
                    remaining_time = "계산 중..."
                
                print(f"\r[{current_time.strftime('%H:%M:%S')}] "
                      f"진행률: {progress:.1f}% "
                      f"({self.stats['processed_files']}/{self.stats['total_files']}) "
                      f"남은 파일: {remaining_files} "
                      f"경과 시간: {str(elapsed_time).split('.')[0]} "
                      f"예상 남은 시간: {remaining_time} "
                      f"처리 중: {self.current_file}", end='')

    def count_total_files(self, path: str) -> int:
        """처리할 총 파일 수 계산"""
        return sum([len(files) for _, _, files in os.walk(path)])

    def rollback_operation(self, operation_id: str) -> bool:
        """작업 롤백"""
        try:
            log_file = self.logger.get_log_file(operation_id)
            if not log_file:
                print(f"작업 ID {operation_id}를 찾을 수 없습니다.")
                return False

            with open(os.path.join(self.logger.log_dir, log_file), 'r') as f:
                log_data = json.load(f)

            if log_data["status"] != "completed":
                print("완료되지 않은 작업은 롤백할 수 없습니다.")
                return False

            # 역순으로 파일 이동 실행
            for move in reversed(log_data["moves"]):
                source = move["destination"]
                destination = move["source"]
                
                if os.path.exists(source):
                    os.makedirs(os.path.dirname(destination), exist_ok=True)
                    shutil.move(source, destination)
                    print(f"롤백: {source} -> {destination}")

            print(f"작업 {operation_id} 롤백 완료")
            return True

        except Exception as e:
            print(f"롤백 중 오류 발생: {e}")
            return False

    def process_up_movement(self, path: str, levels: int, delete_empty: bool) -> None:
        """상위 폴더로 파일 이동 처리"""
        try:
            self.start_operation(path)
            
            for root, dirs, files in os.walk(path, topdown=False):
                while self.is_paused:
                    time.sleep(1)  # CPU 사용률 감소를 위한 대기
                    continue
                    
                relative_path = os.path.relpath(root, path)
                path_parts = relative_path.split(os.sep)
                
                # 지정된 레벨만큼 상위로 이동
                target_level = max(0, len(path_parts) - levels)
                if len(path_parts) > target_level:
                    target_dir = os.path.join(path, *path_parts[:target_level])
                    
                    for file in files:
                        while self.is_paused:
                            time.sleep(1)
                            continue
                            
                        try:
                            current_path = os.path.join(root, file)
                            target_path = os.path.join(target_dir, file)
                            
                            # 파일명 충돌 처리
                            target_path = self.handle_filename_conflict(target_path)
                            
                            # 파일 이동
                            os.makedirs(os.path.dirname(target_path), exist_ok=True)
                            shutil.move(current_path, target_path)
                            self.logger.log_file_move(self.current_operation_id, current_path, target_path)
                            
                            self.stats['processed_files'] += 1
                            self.current_file = file
                            self.update_progress()
                            
                        except Exception as e:
                            self.stats['errors'].append({
                                'file': file,
                                'error': str(e),
                                'timestamp': datetime.datetime.now().isoformat()
                            })
                
                # 빈 폴더 삭제
                if delete_empty and not os.listdir(root):
                    try:
                        os.rmdir(root)
                        print(f"\n빈 폴더 삭제됨: {root}")
                    except Exception as e:
                        print(f"\n폴더 삭제 실패: {root} - {e}")
            
            self.end_operation()
            
        except Exception as e:
            print(f"\n오류 발생: {e}")
            self.cancel_operation()
            raise

    def process_down_movement(self, path: str, delimiters: List[str], chars_to_remove: List[str]) -> None:
        """하위 폴더로 파일 이동 처리"""
        try:
            self.start_operation(path)
            
            for root, _, files in os.walk(path):
                while self.is_paused:
                    time.sleep(1)
                    continue
                    
                for file in files:
                    while self.is_paused:
                        time.sleep(1)
                        continue
                        
                    try:
                        current_path = os.path.join(root, file)
                        filename = os.path.splitext(file)[0]
                        extension = os.path.splitext(file)[1]
                        
                        processed_name = self.remove_chars(filename, chars_to_remove)
                        
                        parts = [processed_name]
                        for delimiter in delimiters:
                            new_parts = []
                            for part in parts:
                                new_parts.extend(part.split(delimiter))
                            parts = new_parts
                        
                        new_path = root
                        for part in parts[:-1]:
                            new_path = os.path.join(new_path, part)
                            os.makedirs(new_path, exist_ok=True)
                        
                        new_filename = parts[-1] + extension
                        target_path = os.path.join(new_path, new_filename)
                        
                        if current_path != target_path:
                            target_path = self.handle_filename_conflict(target_path)
                            shutil.move(current_path, target_path)
                            self.logger.log_file_move(self.current_operation_id, current_path, target_path)
                            print(f"\n이동됨: {current_path} -> {target_path}")
                            
                        self.stats['processed_files'] += 1
                        self.current_file = file
                        self.update_progress()
                        
                    except Exception as e:
                        self.stats['errors'].append({
                            'file': file,
                            'error': str(e),
                            'timestamp': datetime.datetime.now().isoformat()
                        })
            
            self.end_operation()
            
        except Exception as e:
            print(f"\n오류 발생: {e}")
            self.cancel_operation()
            raise

    def main_menu(self) -> None:
        while True:
            self.display_menu()
            choice = input("\n선택하세요: ")
            
            try:
                if choice == "1":
                    self.move_up_structure()
                elif choice == "2":
                    self.move_down_structure()
                elif choice == "3":
                    self.show_operation_history()
                elif choice == "4":
                    self.handle_rollback()
                elif choice == "5":
                    self.show_help()
                elif choice == "6":
                    print("프로그램을 종료합니다.")
                    break
                else:
                    print("잘못된 선택입니다. 다시 선택해주세요.")
            except KeyboardInterrupt:
                if self.handle_keyboard_interrupt():
                    break
            except Exception as e:
                print(f"오류 발생: {e}")

    def get_path_input(self) -> str:
        path = input("대상 경로를 입력하세요: ").strip()
        if not os.path.exists(path):
            raise ValueError("존재하지 않는 경로입니다.")
        return path

    def move_up_structure(self) -> None:
        try:
            path = self.get_path_input()
            levels = int(input("상위로 이동할 레벨을 입력하세요: "))
            delete_empty = input("빈 폴더를 삭제할까요? (y/n): ").lower() == 'y'
            
            # 상위 이동 로직 구현
            self.process_up_movement(path, levels, delete_empty)
            print("상향 이동이 완료되었습니다.")
            
        except Exception as e:
            print(f"오류 발생: {e}")

    def move_down_structure(self) -> None:
        try:
            path = self.get_path_input()
            delimiters = input("구분자들을 입력하세요 (쉼표로 구분): ").split(',')
            chars_to_remove = input("제거할 문자들을 입력하세요 (쉼표로 구분, 생략 가능): ").split(',')
            
            # 하위 이동 로직 구현
            self.process_down_movement(path, delimiters, chars_to_remove)
            print("하향 이동이 완료되었습니다.")
            
        except Exception as e:
            print(f"오류 발생: {e}")

    def process_up_movement(self, path: str, levels: int, delete_empty: bool) -> None:
        """상위 폴더로 파일 이동 처리"""
        try:
            self.start_operation(path)
            
            for root, dirs, files in os.walk(path, topdown=False):
                while self.is_paused:
                    time.sleep(1)  # CPU 사용률 감소를 위한 대기
                    continue
                    
                relative_path = os.path.relpath(root, path)
                path_parts = relative_path.split(os.sep)
                
                # 지정된 레벨만큼 상위로 이동
                target_level = max(0, len(path_parts) - levels)
                if len(path_parts) > target_level:
                    target_dir = os.path.join(path, *path_parts[:target_level])
                    
                    for file in files:
                        while self.is_paused:
                            time.sleep(1)
                            continue
                            
                        try:
                            current_path = os.path.join(root, file)
                            target_path = os.path.join(target_dir, file)
                            
                            # 파일명 충돌 처리
                            target_path = self.handle_filename_conflict(target_path)
                            
                            # 파일 이동
                            os.makedirs(os.path.dirname(target_path), exist_ok=True)
                            shutil.move(current_path, target_path)
                            self.logger.log_file_move(self.current_operation_id, current_path, target_path)
                            
                            self.stats['processed_files'] += 1
                            self.current_file = file
                            self.update_progress()
                            
                        except Exception as e:
                            self.stats['errors'].append({
                                'file': file,
                                'error': str(e),
                                'timestamp': datetime.datetime.now().isoformat()
                            })
                
                # 빈 폴더 삭제
                if delete_empty and not os.listdir(root):
                    try:
                        os.rmdir(root)
                        print(f"\n빈 폴더 삭제됨: {root}")
                    except Exception as e:
                        print(f"\n폴더 삭제 실패: {root} - {e}")
            
            self.end_operation()
            
        except Exception as e:
            print(f"\n오류 발생: {e}")
            self.cancel_operation()
            raise

    @staticmethod
    def remove_chars(filename: str, chars_to_remove: List[str]) -> str:
        for char in chars_to_remove:
            if char:  # 빈 문자열이 아닌 경우에만 처리
                filename = filename.replace(char.strip(), '')
        return filename

    @staticmethod
    def remove_empty_folders(path: str) -> None:
        for root, dirs, files in os.walk(path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):  # 폴더가 비어있는 경우
                        os.rmdir(dir_path)
                except OSError:
                    continue

    def analyze_move_types(self, moves: List[Dict]) -> Dict[str, int]:
        """작업 유형 분석"""
        move_types = {}
        for move in moves:
            source_depth = len(Path(move['source']).parts)
            dest_depth = len(Path(move['destination']).parts)
            
            if source_depth > dest_depth:
                move_type = "상향 이동"
            elif source_depth < dest_depth:
                move_type = "하향 이동"
            else:
                move_type = "수평 이동"
                
            move_types[move_type] = move_types.get(move_type, 0) + 1
        
        return move_types

    def show_operation_history(self):
        """작업 이력 표시"""
        recent_ops = self.logger.get_recent_operations(10)
        if not recent_ops:
            print("\n작업 이력이 없습니다.")
            return
            
        print("\n=== 최근 작업 이력 ===")
        for op in recent_ops:
            status_emoji = "✅" if op["status"] == "completed" else "🔄"
            print(f"\n{status_emoji} 작업 ID: {op['id']}")
            print(f"   시작 시간: {op['timestamp']}")
            if "completed_at" in op:
                print(f"   완료 시간: {op['completed_at']}")
            print(f"   상태: {op['status']}")
            
            # 작업 통계
            total_moves = len(op['moves'])
            print(f"   처리된 파일: {total_moves}개")
            
            # 작업 유형 분석
            if total_moves > 0:
                move_types = self.analyze_move_types(op['moves'])
                print("\n   작업 유형:")
                for move_type, count in move_types.items():
                    print(f"   - {move_type}: {count}개")
            
            # 주요 이동 표시
            if total_moves > 0:
                print("\n   주요 이동:")
                for i, move in enumerate(op['moves'][:3], 1):
                    source = os.path.basename(move['source'])
                    dest = os.path.basename(move['destination'])
                    source_dir = os.path.dirname(move['source'])
                    dest_dir = os.path.dirname(move['destination'])
                    print(f"   {i}. {source} ({source_dir}) -> {dest} ({dest_dir})")
                    print(f"      시간: {move['timestamp']}")
                
                if total_moves > 3:
                    print(f"   ... 외 {total_moves-3}개 파일")
            
        print("\n" + "="*50)

    def handle_rollback(self):
        """롤백 처리"""
        print("\n=== 롤백 실행 ===")
        print("1. 작업 ID로 롤백")
        print("2. 시간으로 롤백")
        
        choice = input("\n선택하세요: ")
        
        if choice == "1":
            operation_id = input("롤백할 작업 ID를 입력하세요: ")
            self.rollback_operation(operation_id)
        elif choice == "2":
            timestamp = input("롤백할 작업 시간을 입력하세요 (YYYYMMDD_HHMMSS 형식): ")
            operation = self.logger.get_operation_by_timestamp(timestamp)
            if operation:
                self.rollback_operation(operation['id'])
            else:
                print("해당 시간의 작업을 찾을 수 없습니다.")

    def show_operation_stats(self):
        """작업 통계 표시"""
        print("\n=== 작업 통계 ===")
        print(f"총 처리 파일: {self.stats['processed_files']}/{self.stats['total_files']}")
        
        # 성공/실패 통계
        success_count = self.stats['processed_files'] - len(self.stats['errors'])
        print(f"성공: {success_count}개")
        print(f"실패: {len(self.stats['errors'])}개")
        
        if self.stats['total_files'] > 0:
            success_rate = (success_count/self.stats['total_files'])*100
            print(f"성공률: {success_rate:.1f}%")
        
        # 작업 시간 통계
        if self.stats['start_time'] and self.stats['end_time']:
            total_time = self.stats['end_time'] - self.stats['start_time']
            effective_time = total_time - self.stats['total_pause_duration']
            print(f"\n시간 통계:")
            print(f"- 총 소요 시간: {str(total_time).split('.')[0]}")
            print(f"- 순수 작업 시간: {str(effective_time).split('.')[0]}")
            print(f"- 일시 정지 시간: {str(self.stats['total_pause_duration']).split('.')[0]}")
            
            if success_count > 0:
                avg_time = effective_time / success_count
                print(f"- 파일당 평균 처리 시간: {str(avg_time).split('.')[0]}")
        
        # 오류 상세 정보
        if self.stats['errors']:
            print(f"\n발생한 오류 ({len(self.stats['errors'])}개):")
            error_types = {}
            for error in self.stats['errors']:
                error_type = type(error['error']).__name__
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            print("\n오류 유형별 통계:")
            for error_type, count in error_types.items():
                print(f"- {error_type}: {count}개")
                
            print("\n상세 오류 목록:")
            for error in self.stats['errors']:
                print(f"- {error['file']}: {error['error']}")
                
        print("\n" + "="*50)

    def handle_filename_conflict(self, target_path: str) -> str:
        """파일명 충돌 처리"""
        if not os.path.exists(target_path):
            return target_path
        
        base, ext = os.path.splitext(target_path)
        counter = 1
        while os.path.exists(f"{base} ({counter}){ext}"):
            counter += 1
        return f"{base} ({counter}){ext}"

    def display_menu(self):
        """개선된 메뉴 표시"""
        print("\n" + "="*50)
        print("        폴더 마법사 (Folder Wizard)")
        print("="*50)
        print("1. 폴더 구조 상향 이동")
        print("   - 하위 폴더의 파일들을 상위로 이동")
        print("2. 폴더 구조 하향 이동")
        print("   - 파일명으로 하위 폴더 자동 생성")
        print("3. 작업 이력 보기")
        print("   - 이전 작업 내역 확인")
        print("4. 작업 롤백")
        print("   - 이전 상태로 복원")
        print("5. 도움말")
        print("   - 프로그램 사용법 안내")
        print("6. 종료")
        print("="*50)

    def cancel_operation(self) -> None:
        """현재 작업 취소"""
        if self.current_operation_id:
            print("\n작업을 취소하는 중...")
            self.rollback_operation(self.current_operation_id)
            self.stats['end_time'] = datetime.datetime.now()
            print("\n작업이 취소되었습니다.")
            self.show_operation_stats()

    def start_operation(self, path: str) -> None:
        """작업 시작 설정"""
        self.current_operation_id = self.logger.create_operation_log()
        self.start_time = datetime.datetime.now()
        self.stats = {
            'total_files': self.count_total_files(path),
            'processed_files': 0,
            'errors': [],
            'start_time': self.start_time,
            'end_time': None
        }

    def end_operation(self) -> None:
        """작업 종료 처리"""
        if self.current_operation_id:
            self.stats['end_time'] = datetime.datetime.now()
            self.logger.complete_operation(self.current_operation_id)
            print("\n작업이 완료되었습니다.")
            self.show_operation_stats()

    def pause_operation(self) -> None:
        """작업 일시 정지"""
        if not self.is_paused:
            self.is_paused = True
            self.stats['pause_time'] = datetime.datetime.now()
            print("\n작업이 일시 정지되었습니다.")
            print("계속하려면 'r'을 입력하세요.")

    def resume_operation(self) -> None:
        """작업 재개"""
        if self.is_paused:
            pause_duration = datetime.datetime.now() - self.stats['pause_time']
            self.stats['total_pause_duration'] += pause_duration
            self.is_paused = False
            self.stats['pause_time'] = None
            print("\n작업을 재개합니다.")

    def handle_keyboard_interrupt(self) -> bool:
        """Ctrl+C 처리"""
        print("\n\n작업을 어떻게 하시겠습니까?")
        print("1. 일시 정지")
        print("2. 작업 취소")
        print("3. 계속 진행")
        
        choice = input("선택하세요 (1-3): ").strip()
        
        if choice == "1":
            self.pause_operation()
            while self.is_paused:
                if input().lower() == 'r':
                    self.resume_operation()
                    return False
            return False
        elif choice == "2":
            if input("정말로 작업을 취소하시겠습니까? (y/n): ").lower() == 'y':
                self.cancel_operation()
                return True
        return False

    def show_help(self) -> None:
        """도움말 표시"""
        print("\n=== 폴더 마법사 도움말 ===")
        
        print("\n1. 폴더 구조 상향 이동")
        print("   - 하위 폴더의 파일들을 상위 폴더로 이동합니다.")
        print("   - 이동 레벨을 지정하여 원하는 상위 폴더로 이동할 수 있습니다.")
        print("   - 파일명 충돌 시 자동으로 번호를 붙여 처리합니다.")
        print("   - 빈 폴더 자동 삭제 옵션을 선택할 수 있습니다.")
        print("\n   사용 예시:")
        print("     1) 상위 1단계로 이동: 문서/2024/1월/파일.txt -> 문서/2024/파일.txt")
        print("     2) 상위 2단계로 이동: 문서/2024/1월/파일.txt -> 문서/파일.txt")
        
        print("\n2. 폴더 구조 하향 이동")
        print("   - 파일명을 기준으로 하위 폴더를 자동 생성합니다.")
        print("   - 여러 구분자를 사용하여 복잡한 폴더 구조를 만들 수 있습니다.")
        print("   - 파일명에서 불필요한 문자를 제거할 수 있습니다.")
        print("\n   사용 예시:")
        print("       1) 단일 구분자: 2024_01_회의록.txt -> 2024/01/회의록.txt")
        print("       2) 다중 구분자: 2024-01_회의-기획.txt -> 2024/01/회의/기획.txt")
        print("       3) 문자 제거: IMG_2024_01.jpg -> 2024/01.jpg (IMG_ 제거)")
        
        print("\n3. 작업 관리")
        print("   - 작업 진행 상황을 실시간으로 확인할 수 있습니다.")
        print("   - 작업 중 일시 정지 및 재개가 가능합니다. (Ctrl + C)")
        print("   - 문제 발생 시 이전 상태로 롤백할 수 있습니다.")
        print("   - 작업 이력을 조회하고 관리할 수 있습니다.")
        print("   - 상세한 작업 통계를 확인할 수 있습니다.")
        print("\n   통계 정보:")
        print("   - 성공/실패 건수 및 비율")
        print("   - 총 소요 시간 및 순수 작업 시간")
        print("   - 파일당 평균 처리 시간")
        print("   - 오류 유형별 통계")
        
        print("\n4. 단축키 및 명령어")
        print("   - Ctrl+C: 작업 일시 정지 또는 취소")
        print("   - R: 일시 정지된 작업 재개")
        print("   - Q: 프로그램 종료")
        
        print("\n5. 안전 기능")
        print("   - 모든 작업은 로그에 기록되어 추적 가능")
        print("   - 작업 실패 시 자동 롤백")
        print("   - 파일명 충돌 자동 해결")
        print("   - 작업 중단 및 재개 지원")
        
        print("\n자세한 내용은 https://github.com/your-repo/folder-wizard 를 참조하세요.")
        print("\n" + "="*50)

if __name__ == "__main__":
    wizard = FolderWizard()
    wizard.main_menu()